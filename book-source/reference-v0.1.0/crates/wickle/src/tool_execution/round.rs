use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, time::Duration};

impl SerialToolRound {
    /// Execute only calls from one committed physical model response, in saved order.
    /// Existing settled results are reused, and uncertain attempts are never retried.
    pub async fn execute(
        &self,
        model_request_id: &Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        self.scope(context, budget)?;
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        if let Some(entry) = saved.snapshot.tool_ledger.iter().find(|entry| {
            matches!(entry.state, ToolCallState::Unknown { .. } | ToolCallState::Dispatching { .. })
                || matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Unknown || result.effect == ToolEffect::Unknown)
        }) {
            return self.existing_uncertainty(entry, budget).await;
        }
        if let Some(request) = saved.snapshot.tool_ledger.iter().find_map(|entry| {
            if let ToolCallState::InputPending { request, .. } = &entry.state {
                Some(request.clone())
            } else {
                None
            }
        }) {
            return Ok(ToolRoundOutcome::InputRequired { request });
        }
        let call_ids: Vec<_> = saved
            .snapshot
            .tool_ledger
            .iter()
            .filter(|entry| &entry.call.model_request_id == model_request_id)
            .map(|entry| entry.call.call_id.clone())
            .collect();
        for call_id in call_ids {
            self.boundary(context, budget).await?;
            let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
            let entry = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| entry.call.call_id == call_id)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
            match &entry.state {
                ToolCallState::Settled { result }
                    if result.status != ToolResultStatus::Unknown
                        && result.effect != ToolEffect::Unknown =>
                {
                    continue;
                }
                ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. } => {}
                ToolCallState::InputPending { request, .. } => {
                    return Ok(ToolRoundOutcome::InputRequired {
                        request: request.clone(),
                    });
                }
                _ => return self.existing_uncertainty(entry, budget).await,
            }
            let call = entry.call.clone();
            let pending_key = if let ToolCallState::ApprovalPending {
                idempotency_key, ..
            } = &entry.state
            {
                Some(idempotency_key.clone())
            } else {
                None
            };
            let call_message_id = call_message(&saved, &call)?;
            let registered = self.registry.get(&call.tool_name);
            let Some(registered) = registered.filter(|entry| {
                call.descriptor_digest.as_ref() == Some(entry.compiled.descriptor_digest())
            }) else {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "unknown_tool",
                    budget,
                    context,
                )
                .await?;
                continue;
            };
            if registered
                .compiled
                .validate_model_inputs(&call.model_inputs)
                .is_err()
            {
                self.reject(
                    &call,
                    call_message_id,
                    ToolResultStatus::Failed,
                    "invalid_arguments",
                    budget,
                    context,
                )
                .await?;
                continue;
            }
            if call.bound_input_ref.is_none() {
                if let Some(hooks) = &self.hooks {
                    let transformed = hooks
                        .transform(
                            HookTarget::BeforeTool {
                                call_id: call_id.clone(),
                            },
                            HookInput::BeforeTool {
                                tool: registered.compiled.to_model_tool(),
                                descriptor_digest: registered.compiled.descriptor_digest().clone(),
                                compiled_digest: registered.compiled.digest().clone(),
                                original_model_inputs: call.model_inputs.clone(),
                                model_inputs: call.model_inputs.clone(),
                            },
                            context,
                            budget,
                        )
                        .await?;
                    if let Some(reason) = transformed.deny {
                        self.reject(
                            &call,
                            call_message_id,
                            ToolResultStatus::Denied,
                            reason.as_str(),
                            budget,
                            context,
                        )
                        .await?;
                        continue;
                    }
                    if let Some(inputs) = transformed.model_inputs {
                        registered.compiled.validate_model_inputs(&inputs)?;
                    }
                }
            }
            let bound = match crate::future::boxed(|| {
                self.binder
                    .bind(&registered.compiled, &call_id, context, budget)
            })
            .await
            {
                Ok(bound) => bound,
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    let status = if error.code == ErrorCode::AccessDenied {
                        ToolResultStatus::Denied
                    } else {
                        ToolResultStatus::Failed
                    };
                    self.reject(
                        &call,
                        call_message_id,
                        status,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
            };
            if let PolicyDecision::RequireApproval { reason } = bound.decision {
                return Ok(ToolRoundOutcome::ApprovalRequired {
                    call_id,
                    reason,
                    bound_input_ref: bound.reference,
                    binding_digest: bound.input.binding_digest().clone(),
                });
            }
            match self.authorize(&bound.input, context, budget).await? {
                PolicyDecision::RequireApproval { reason } => {
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                PolicyDecision::Deny { .. } => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
                PolicyDecision::Allow {} => {}
            }
            let reservation = budget
                .reserve(ReservationKind::Tool {
                    call_id: call_id.clone(),
                })
                .await?;
            let key = if let Some(key) = pending_key {
                key
            } else {
                Id::new(format!(
                    "tool-effect-{}",
                    canonical_digest(&serde_json::json!([
                        budget.scope(),
                        budget.run_id(),
                        call_id,
                        bound.input.binding_digest()
                    ]))
                ))?
            };
            self.dispatching(&call_id, &reservation.attempt_id, &key, budget)
                .await?;
            let gate = async {
                self.boundary(context, budget).await?;
                let decision = self.authorize(&bound.input, context, budget).await?;
                self.boundary(context, budget).await?;
                Ok::<_, ContractError>(decision)
            }
            .await;
            match gate {
                Ok(PolicyDecision::Allow {}) => {}
                Ok(PolicyDecision::RequireApproval { reason }) => {
                    self.approval_pending(&call_id, &reservation.attempt_id, &key, budget)
                        .await?;
                    return Ok(ToolRoundOutcome::ApprovalRequired {
                        call_id,
                        reason,
                        bound_input_ref: bound.reference,
                        binding_digest: bound.input.binding_digest().clone(),
                    });
                }
                Ok(PolicyDecision::Deny { .. }) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        "access_denied",
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
                    ) =>
                {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) if control_or_storage(error.code) => return Err(error),
                Err(error) => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Denied,
                        &code_name(error.code),
                        budget,
                        context,
                    )
                    .await?;
                    continue;
                }
            }
            let cancellation = budget.cancellation().child_token();
            let _cancel = cancellation.clone().drop_guard();
            let run_deadline = match budget.call_deadline() {
                Ok(deadline) => deadline,
                Err(error) if error.code == ErrorCode::DeadlineExceeded => {
                    self.reject(
                        &call,
                        call_message_id,
                        ToolResultStatus::Cancelled,
                        "deadline_exceeded",
                        budget,
                        context,
                    )
                    .await?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_millis(self.limits.timeout_ms))
                .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "tool.timeout"))?
                .min(run_deadline);
            let execution = ToolExecutionContext {
                run_id: budget.run_id().clone(),
                binding_set_id: self.binding_set_id.clone(),
                call_id: call_id.clone(),
                attempt_id: reservation.attempt_id.clone(),
                idempotency_key: key.clone(),
                scope: budget.scope().clone(),
                principal_ref: context.data.principal_ref.clone(),
                capability_grant_ref: context.data.capability_grant_ref.clone(),
                cancellation,
                deadline,
            };
            let mut entered = false;
            let result = {
                let operation = AssertUnwindSafe(async {
                    entered = true;
                    registered
                        .executor
                        .execute(bound.input.execution_args(), &execution)
                        .await
                })
                .catch_unwind();
                tokio::select! { biased;
                    _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.execution")),
                    _ = tokio::time::sleep_until(deadline) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")),
                    stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.execution")) },
                    result = operation => result.unwrap_or_else(|_| Err(error(ErrorCode::InvalidContract, "tool.executor"))),
                }
            };
            execution.cancellation.cancel();
            let completion = match result {
                Ok(result) => result,
                Err(error) => ToolExecutionResult {
                    outcome: ToolExecutionOutcome::Failed {
                        code: Id::new(code_name(error.code))?,
                    },
                    effect: if entered
                        && registered.compiled.descriptor().side_effect != ToolSideEffect::ReadOnly
                    {
                        ToolEffect::Unknown
                    } else {
                        ToolEffect::NotApplied
                    },
                    receipt: None,
                },
            };
            if let ToolExecutionOutcome::InputRequired { question } = &completion.outcome {
                let question_bytes = serde_json::to_vec(question)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.input_question"))?
                    .len();
                if completion.effect == ToolEffect::NotApplied
                    && completion.receipt.is_none()
                    && !question.trim().is_empty()
                    && question_bytes as u64
                        <= registered.compiled.descriptor().max_output_bytes.get()
                {
                    let request = InputRequest {
                        input_request_id: self.ids.next_id()?,
                        call_id: call_id.clone(),
                        question: question.clone(),
                        schema_ref: None,
                    };
                    self.input_pending(&execution, &request, budget).await?;
                    return Ok(ToolRoundOutcome::InputRequired { request });
                }
            }
            let (mut result, records) = self.validate_output(
                &call,
                call_message_id,
                AttemptIdentity {
                    scope: &execution.scope,
                    run_id: &execution.run_id,
                    attempt_id: &execution.attempt_id,
                    idempotency_key: &execution.idempotency_key,
                },
                &registered.compiled,
                &bound.input,
                completion,
            )?;
            self.validate_artifact_result(
                &mut result,
                context,
                budget
                    .call_deadline()
                    .unwrap_or_else(|_| tokio::time::Instant::now()),
            )
            .await;
            let unresolved = result.effect == ToolEffect::Unknown;
            let state = if unresolved {
                ToolCallState::Unknown {
                    attempt_id: execution.attempt_id.clone(),
                    idempotency_key: key.clone(),
                }
            } else {
                ToolCallState::Settled {
                    result: result.clone(),
                }
            };
            let result_ref = self
                .settle(&call_id, state, result, records, budget, context)
                .await?;
            if unresolved {
                return Ok(ToolRoundOutcome::Unresolved {
                    call_id,
                    result_ref,
                });
            }
        }
        Ok(ToolRoundOutcome::Completed)
    }

    /// Close unstarted plans and input requests confirmed to have no effect when
    /// a segment ends. Unknown and potentially dispatched calls are untouched.
    pub async fn settle_unstarted(
        &self,
        model_request_id: &Id,
        status: ToolResultStatus,
        code: Id,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if !matches!(
            status,
            ToolResultStatus::Failed | ToolResultStatus::Denied | ToolResultStatus::Cancelled
        ) {
            return Err(error(ErrorCode::InvalidContract, "tool.settlement"));
        }
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        for entry in &saved.snapshot.tool_ledger {
            if &entry.call.model_request_id == model_request_id
                && matches!(
                    entry.state,
                    ToolCallState::Planned {}
                        | ToolCallState::ApprovalPending { .. }
                        | ToolCallState::InputPending { .. }
                )
            {
                self.reject(
                    &entry.call,
                    call_message(&saved, &entry.call)?,
                    status,
                    code.as_str(),
                    budget,
                    context,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn scope(&self, context: &ExecutionContext, budget: &RunBudget) -> Result<(), ContractError> {
        if &context.data.scope != budget.scope() || self.registry.scope() != budget.scope() {
            return Err(error(ErrorCode::AccessDenied, "scope"));
        }
        Ok(())
    }
    async fn boundary(
        &self,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        self.scope(context, budget)?;
        if context.cancellation.is_cancelled() {
            return Err(error(ErrorCode::Cancelled, "tool"));
        }
        budget.check_boundary().await
    }
    async fn authorize(
        &self,
        input: &BoundToolInput,
        context: &ExecutionContext,
        budget: &RunBudget,
    ) -> Result<PolicyDecision, ContractError> {
        let saved = tokio::select! { biased;
            _ = context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => return match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = budget.store().load(budget.scope(), budget.run_id()) => result?,
        };
        let request = input.policy_request_for_run(&saved.snapshot);
        tokio::select! { biased;
            _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "tool.policy")),
            stopped = budget.wait_for_cancellation_or_deadline() => match stopped { Err(error) => Err(error), Ok(()) => Err(error(ErrorCode::DeadlineExceeded, "tool.policy")) },
            result = self.policy.check(&request, context, Some(budget.call_deadline()?), None) => result,
        }
    }
    async fn dispatching(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(
            entry.state,
            ToolCallState::Planned {} | ToolCallState::ApprovalPending { .. }
        ) || entry.call.bound_input_ref.is_none()
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.dispatch"));
        }
        if let ToolCallState::ApprovalPending {
            idempotency_key, ..
        } = &entry.state
        {
            if idempotency_key != key {
                return Err(error(ErrorCode::InvalidTransition, "tool.idempotency_key"));
            }
        }
        entry.state = ToolCallState::Dispatching {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn approval_pending(
        &self,
        call_id: &Id,
        attempt_id: &Id,
        key: &Id,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id: current, idempotency_key } if current == attempt_id && idempotency_key == key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.approval"));
        }
        entry.state = ToolCallState::ApprovalPending {
            attempt_id: attempt_id.clone(),
            idempotency_key: key.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn input_pending(
        &self,
        execution: &ToolExecutionContext,
        request: &InputRequest,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| entry.call.call_id == execution.call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if !matches!(&entry.state, ToolCallState::Dispatching { attempt_id, idempotency_key }
            if attempt_id == &execution.attempt_id && idempotency_key == &execution.idempotency_key)
        {
            return Err(error(ErrorCode::InvalidTransition, "tool.input_pending"));
        }
        entry.state = ToolCallState::InputPending {
            attempt_id: execution.attempt_id.clone(),
            idempotency_key: execution.idempotency_key.clone(),
            request: request.clone(),
        };
        self.commit(snapshot, vec![], vec![], vec![], budget).await
    }
    async fn reject(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        status: ToolResultStatus,
        code: &str,
        budget: &RunBudget,
        context: &ExecutionContext,
    ) -> Result<(), ContractError> {
        let result = ToolResult {
            call_id: call.call_id.clone(),
            call_message_id,
            status,
            effect: ToolEffect::NotApplied,
            content: vec![],
            effect_receipt_ref: None,
            skill_ref: None,
            error: Some(Failure {
                code: Id::new(code)?,
                diagnostic_ref: None,
            }),
        };
        self.settle(
            &call.call_id,
            ToolCallState::Settled {
                result: result.clone(),
            },
            result,
            vec![],
            budget,
            context,
        )
        .await?;
        Ok(())
    }

    pub(super) fn validate_output(
        &self,
        call: &ToolCall,
        call_message_id: Id,
        attempt: AttemptIdentity<'_>,
        compiled: &CompiledTool,
        bound: &BoundToolInput,
        completion: ToolExecutionResult,
    ) -> Result<(ToolResult, Vec<ProtectedRecord>), ContractError> {
        let effect = completion.effect;
        let receipt_bytes = completion
            .receipt
            .as_ref()
            .map(|receipt| serde_json::to_vec(receipt).map(|bytes| bytes.len()))
            .transpose()
            .map_err(|_| error(ErrorCode::InvalidJson, "tool.receipt"))?;
        let receipt_oversized =
            receipt_bytes.is_some_and(|size| size > self.limits.max_receipt_bytes);
        let raw_receipt = if receipt_oversized {
            serde_json::json!({"omitted":true,"bytes":receipt_bytes,"digest":canonical_digest(completion.receipt.as_ref().expect("oversized receipt"))})
        } else {
            completion.receipt.clone().unwrap_or(Value::Null)
        };
        let mut status = ToolResultStatus::Succeeded;
        let mut code = None;
        let mut content = vec![];
        let mut skill_record = None;
        let configured_loader = self.skill_plan.as_ref().is_some_and(|plan| {
            plan.loader_digest() == compiled.descriptor_digest()
                && self.registry.selection(&call.tool_name) == Some(plan.loader())
        });
        let raw_output = match &completion.outcome {
            ToolExecutionOutcome::Succeeded { .. }
            | ToolExecutionOutcome::SucceededWithContent { .. }
                if configured_loader =>
            {
                status = ToolResultStatus::Failed;
                code = Some(Id::new("skill_loader_contract")?);
                Value::Null
            }
            ToolExecutionOutcome::LoadedSkill { loaded } => {
                let valid = self.skill_plan.as_ref().is_some_and(|plan| {
                    plan.loader_digest() == compiled.descriptor_digest()
                        && self.registry.selection(&call.tool_name) == Some(plan.loader())
                        && loaded.validate(plan, attempt.run_id, &call.call_id).is_ok()
                        && loaded.matches_input(bound)
                });
                if !valid || effect != ToolEffect::NotApplied || completion.receipt.is_some() {
                    status = ToolResultStatus::Failed;
                    code = Some(Id::new("invalid_skill_result")?);
                    Value::Null
                } else {
                    let stored = serde_json::to_value(loaded)
                        .map_err(|_| error(ErrorCode::InvalidJson, "skill.result"))?;
                    let bytes = serde_json::to_vec(&stored)
                        .map_err(|_| error(ErrorCode::InvalidJson, "skill.result"))?
                        .len();
                    if bytes as u64 > compiled.descriptor().max_output_bytes.get() {
                        status = ToolResultStatus::Failed;
                        code = Some(Id::new("tool_output_too_large")?);
                        serde_json::json!({"omitted":true,"bytes":bytes,"digest":canonical_digest(&stored)})
                    } else {
                        let value = loaded.summary();
                        content.push(InputContent::Json {
                            value: value.clone(),
                        });
                        skill_record = Some(ProtectedRecord::new(self.ids.next_id()?, 1, stored));
                        value
                    }
                }
            }
            ToolExecutionOutcome::Succeeded { value }
            | ToolExecutionOutcome::SucceededWithContent { value, .. } => {
                let extra = match &completion.outcome {
                    ToolExecutionOutcome::SucceededWithContent { content, .. } => {
                        content.as_slice()
                    }
                    _ => &[],
                };
                let bytes = serde_json::to_vec(value)
                    .map_err(|_| error(ErrorCode::InvalidJson, "tool.output"))?
                    .len()
                    .saturating_add(if extra.is_empty() {
                        0
                    } else {
                        serde_json::to_vec(extra)
                            .map_err(|_| error(ErrorCode::InvalidJson, "tool.content"))?
                            .len()
                    });
                if bytes as u64 > compiled.descriptor().max_output_bytes.get() {
                    status = ToolResultStatus::Failed;
                    code = Some(Id::new("tool_output_too_large")?);
                    serde_json::json!({"omitted":true,"bytes":bytes,"digest":canonical_digest(value)})
                } else {
                    if !crate::tool_schema::compile_validator(&compiled.descriptor().output_schema)?.is_valid(value) || extra.iter().any(|item|matches!(item,InputContent::Artifact {reference} if &reference.scope!=attempt.scope))
                    {
                        status = ToolResultStatus::Failed;
                        code = Some(Id::new("invalid_tool_output")?);
                    } else {
                        content.push(InputContent::Json {
                            value: value.clone(),
                        });
                        content.extend_from_slice(extra);
                    }
                    value.clone()
                }
            }
            ToolExecutionOutcome::Failed { code: failure } => {
                status = if failure.as_str() == "cancelled" {
                    ToolResultStatus::Cancelled
                } else {
                    ToolResultStatus::Failed
                };
                code = Some(failure.clone());
                Value::Null
            }
            ToolExecutionOutcome::InputRequired { .. } => {
                // Only a bounded no-effect request is accepted before this path.
                // Invalid requests retain any reported effect and receipt.
                status = ToolResultStatus::Failed;
                code = Some(Id::new("invalid_input_request")?);
                Value::Null
            }
        };
        if effect == ToolEffect::Unknown {
            status = ToolResultStatus::Unknown;
            code = Some(Id::new("tool_effect_unknown")?);
            content.clear();
        } else if receipt_oversized {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_too_large")?);
            content.clear();
        } else if effect == ToolEffect::Applied && completion.receipt.is_none() {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("effect_receipt_missing")?);
            content.clear();
        } else if effect == ToolEffect::Applied
            && compiled.descriptor().side_effect == ToolSideEffect::ReadOnly
        {
            status = ToolResultStatus::Failed;
            code = Some(Id::new("tool_effect_contract")?);
            content.clear();
        }
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::json!({
                "scope":attempt.scope,"call_id":call.call_id,"attempt_id":attempt.attempt_id,"idempotency_key":attempt.idempotency_key,
                "effect":effect,"receipt":raw_receipt,"receipt_omitted":receipt_oversized,"output":raw_output,"error_code":code,
            }),
        );
        let reference = record.reference().clone();
        if status != ToolResultStatus::Succeeded {
            skill_record = None;
        }
        let skill_ref = skill_record
            .as_ref()
            .map(|record| record.reference().clone());
        let mut records = vec![record];
        records.extend(skill_record);
        Ok((
            ToolResult {
                call_id: call.call_id.clone(),
                call_message_id,
                status,
                effect,
                content,
                effect_receipt_ref: (effect != ToolEffect::NotApplied
                    || completion.receipt.is_some())
                .then(|| reference.clone()),
                skill_ref,
                error: code.map(|code| Failure {
                    code,
                    diagnostic_ref: Some(reference),
                }),
            },
            records,
        ))
    }

    pub(super) async fn settle(
        &self,
        call_id: &Id,
        state: ToolCallState,
        result: ToolResult,
        mut records: Vec<ProtectedRecord>,
        budget: &RunBudget,
        context: &ExecutionContext,
    ) -> Result<RecordRef, ContractError> {
        let saved = budget.store().load(budget.scope(), budget.run_id()).await?;
        let mut snapshot = saved.snapshot;
        let entry = snapshot
            .tool_ledger
            .iter_mut()
            .find(|entry| &entry.call.call_id == call_id)
            .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.call"))?;
        if matches!(
            entry.state,
            ToolCallState::Settled { .. } | ToolCallState::Unknown { .. }
        ) {
            return Err(error(ErrorCode::InvalidTransition, "tool.settlement"));
        }
        entry.state = state.clone();
        let intended_state = state.clone();
        let observer_input = HookInput::tool_observed(call_id, &result);
        let record = ProtectedRecord::new(
            self.ids.next_id()?,
            1,
            serde_json::to_value(&result)
                .map_err(|_| error(ErrorCode::InvalidJson, "tool.result"))?,
        );
        let reference = record.reference().clone();
        let intended_value = record.value().clone();
        records.push(record);
        snapshot.last_event_seq = snapshot
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidEvent, "tool.event"))?;
        let (_, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let event = RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: self.ids.next_id()?,
            scope: budget.scope().clone(),
            run_id: budget.run_id().clone(),
            session_id: snapshot.request.session_id.clone(),
            seq: snapshot
                .last_event_seq
                .try_into()
                .map_err(|_| error(ErrorCode::InvalidEvent, "tool.event"))?,
            timestamp_ms: now,
            payload: match state {
                ToolCallState::Unknown {
                    attempt_id,
                    idempotency_key,
                } => RunEventPayload::ToolUnresolved {
                    result_ref: reference.clone(),
                    attempt_id,
                    idempotency_key,
                },
                _ => RunEventPayload::ToolSettled {
                    result_ref: reference.clone(),
                },
            },
        };
        let message = Message {
            message_id: self.ids.next_id()?,
            run_id: budget.run_id().clone(),
            sequence: saved
                .session
                .transcript_revision
                .checked_add(1)
                .and_then(std::num::NonZeroU64::new)
                .ok_or_else(|| error(ErrorCode::InvalidSnapshot, "tool.message"))?,
            role: MessageRole::Tool,
            content: vec![ContentBlock::ToolResult { result }],
            origin: MessageOrigin::Tool,
            visibility: Visibility::UserAndModel,
        };
        let committed = self
            .commit(snapshot, vec![message], vec![event], records, budget)
            .await;
        if let Err(error) = committed {
            let restored = budget.store().read_record(budget.scope(), &reference).await;
            if !restored.is_ok_and(|record| {
                record.reference() == &reference && record.value() == &intended_value
            }) {
                return Err(error);
            }
            let Ok(saved) = budget.store().load(budget.scope(), budget.run_id()).await else {
                return Err(error);
            };
            let found = saved
                .snapshot
                .tool_ledger
                .iter()
                .find(|entry| &entry.call.call_id == call_id);
            if !found.is_some_and(|entry| entry.state == intended_state) {
                return Err(error);
            }
        }
        if let Some(hooks) = &self.hooks {
            let mut data = context.data.clone();
            data.system_inputs = None;
            let cleanup = ExecutionContext::new(data, CancellationToken::new());
            if let Err(error) = hooks
                .observe(
                    budget.run_id(),
                    HookTarget::AfterTool {
                        call_id: call_id.clone(),
                        result_ref: reference.clone(),
                    },
                    observer_input,
                    &cleanup,
                )
                .await
            {
                if let Ok(mut slot) = self.observer_error.lock() {
                    *slot = Some(ContractError::new(error.code, "hooks.observer_report"));
                }
            }
        }
        Ok(reference)
    }
    pub(super) async fn commit(
        &self,
        mut snapshot: RunSnapshot,
        messages: Vec<Message>,
        events: Vec<RunEvent>,
        records: Vec<ProtectedRecord>,
        budget: &RunBudget,
    ) -> Result<(), ContractError> {
        let expected_revision = snapshot.revision;
        let (_, check_at) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        let lease = budget
            .store()
            .check_lease(budget.scope(), budget.run_id(), budget.lease(), check_at)
            .await?;
        let (elapsed, now) = budget.settlement_time(snapshot.usage.elapsed_ms)?;
        if now >= lease.expires_at_ms {
            return Err(error(ErrorCode::LeaseLost, "tool.lease"));
        }
        snapshot.revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::RevisionConflict, "tool.revision"))?;
        snapshot.phase = RunPhase::Tool;
        snapshot.usage.elapsed_ms = elapsed;
        snapshot.timing.last_observed_at_ms = now;
        budget
            .store()
            .commit(
                budget.scope(),
                budget.run_id(),
                CommitInput {
                    expected_revision,
                    lease: budget.lease().clone(),
                    now_ms: now,
                    snapshot,
                    messages,
                    events,
                    records,
                },
            )
            .await?;
        Ok(())
    }
    async fn existing_uncertainty(
        &self,
        entry: &ToolLedgerEntry,
        budget: &RunBudget,
    ) -> Result<ToolRoundOutcome, ContractError> {
        let ToolCallState::Unknown {
            attempt_id,
            idempotency_key,
        } = &entry.state
        else {
            return Err(error(
                ErrorCode::InvalidTransition,
                "tool.unresolved_dispatch",
            ));
        };
        let mut after = 0;
        loop {
            let page = budget
                .store()
                .read_events(budget.scope(), budget.run_id(), after, MAX_EVENT_PAGE_SIZE)
                .await?;
            for event in &page.events {
                if let RunEventPayload::ToolUnresolved {
                    result_ref,
                    attempt_id: saved_attempt,
                    idempotency_key: saved_key,
                } = &event.payload
                {
                    if saved_attempt == attempt_id && saved_key == idempotency_key {
                        return Ok(ToolRoundOutcome::Unresolved {
                            call_id: entry.call.call_id.clone(),
                            result_ref: result_ref.clone(),
                        });
                    }
                }
            }
            if !page.has_more {
                return Err(error(ErrorCode::InvalidSnapshot, "tool.unresolved_result"));
            }
            after = page.next_after_seq;
        }
    }
}

pub(crate) fn call_message(saved: &StoredRun, call: &ToolCall) -> Result<Id, ContractError> {
    let messages: Vec<_> = saved.messages.iter().filter(|message| message.run_id == saved.snapshot.run_id && message.role == MessageRole::Assistant && message.content.iter().any(|content| matches!(content, ContentBlock::ToolCall { call: candidate } if candidate.call_id == call.call_id && candidate.model_request_id == call.model_request_id && candidate.provider_call_id == call.provider_call_id && candidate.tool_name == call.tool_name && candidate.model_inputs == call.model_inputs && candidate.descriptor_digest == call.descriptor_digest))).collect();
    if messages.len() != 1 {
        return Err(error(ErrorCode::InvalidSnapshot, "tool.call_message"));
    }
    Ok(messages[0].message_id.clone())
}
fn code_name(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "invalid_contract".into())
}
fn control_or_storage(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Cancelled
            | ErrorCode::DeadlineExceeded
            | ErrorCode::BudgetExceeded
            | ErrorCode::LeaseLost
            | ErrorCode::RevisionConflict
            | ErrorCode::PersistenceUnavailable
            | ErrorCode::StateNotFound
            | ErrorCode::ClockUnavailable
            | ErrorCode::ClockRegression
    )
}
