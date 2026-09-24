//! Waiting segments resume through durable commands without repeating saved tool effects.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;
use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use wickle::*;

#[tokio::test]
async fn approval_continues_the_same_run_with_frozen_inputs_and_a_distinct_segment_handle() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let saved_wait = fixture.saved(&original).await;
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before"]);
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 1);
    let command = fixture.approve(&original, "approve").await;
    *fixture.resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(CHANGED_RECORD),
        revision: id("record-revision-2"),
    };
    let mut approver = context();
    approver.data.principal_ref = id("reviewer");
    approver.data.capability_grant_ref = id("reviewer-grant");
    let resumed = completed(
        agent
            .resume(command.clone(), approver.clone())
            .await
            .unwrap(),
    );
    let result = fixture.outcome(&resumed).await;
    assert_eq!(resumed.run_id(), original.run_id());
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert!(result.checkpoint_revision > waiting.checkpoint_revision);
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["before", "target", "after"]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    {
        let observed = fixture.tools[1].seen.lock().unwrap();
        assert_eq!(
            observed[0].arguments,
            object(json!({"query":"target","workspace_id":WORKSPACE,"record_id":RECORD}))
        );
        assert_eq!(
            observed[0].context.principal_ref,
            approver.data.principal_ref
        );
        assert_eq!(
            observed[0].context.capability_grant_ref,
            approver.data.capability_grant_ref
        );
    }
    let saved = fixture.saved(&resumed).await;
    assert_eq!(saved.snapshot.request, saved_wait.snapshot.request);
    assert_eq!(
        saved.snapshot.system_inputs,
        saved_wait.snapshot.system_inputs
    );
    assert_eq!(
        saved.session.prompt_snapshot,
        saved_wait.session.prompt_snapshot
    );
    assert_eq!(
        saved.snapshot.tool_ledger[1].call,
        saved_wait.snapshot.tool_ledger[1].call
    );
    let receipt = &saved.snapshot.resume_receipts[0];
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(receipt.command, command);
    assert_eq!(receipt.actor_ref, approver.data.principal_ref);
    assert_eq!(
        receipt.previous_last_event_seq,
        saved_wait.snapshot.last_event_seq
    );
    assert_eq!(receipt.previous_segment_start_revision, 0);
    let prior = fixture
        .base
        .store
        .read_record(&scope(), &receipt.previous_outcome_ref)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<RunOutcome>(prior.value().clone()).unwrap(),
        waiting
    );
    assert_eq!(fixture.outcome(&original).await, waiting);
    let old_events: Vec<_> = original.events(0, context()).try_collect().await.unwrap();
    assert_eq!(
        old_events.last().unwrap().seq.get(),
        saved_wait.snapshot.last_event_seq
    );
    let remaining: Vec<_> = resumed
        .events(saved_wait.snapshot.last_event_seq, context())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        remaining[0].seq.get(),
        saved_wait.snapshot.last_event_seq + 1
    );
    assert_eq!(
        remaining.last().unwrap().seq.get(),
        saved.snapshot.last_event_seq
    );
    let events = fixture
        .base
        .store
        .read_events(&scope(), original.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(
        events
            .events
            .windows(2)
            .all(|pair| pair[1].seq.get() == pair[0].seq.get() + 1)
    );
    assert_eq!(
        events
            .events
            .iter()
            .filter(|event| matches!(event.payload, RunEventPayload::RunResumed { .. }))
            .count(),
        1
    );
    assert!(
        fixture
            .policy
            .tool_checks
            .lock()
            .unwrap()
            .iter()
            .any(|(input, principal)| input
                .approval()
                .is_some_and(|approval| approval.command_id() == &command.command_id
                    && approval.actor_ref() == &approver.data.principal_ref)
                && principal == &approver.data.principal_ref)
    );
}

#[tokio::test(start_paused = true)]
async fn waiting_consumes_no_new_calls_and_late_approval_keeps_its_charged_reservation() {
    let fixture = Fixture::new(Mode::LateApproval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let saved = fixture.saved(&original).await;
    let ToolCallState::ApprovalPending {
        attempt_id,
        idempotency_key,
    } = &saved.snapshot.tool_ledger[1].state
    else {
        panic!("expected reserved approval")
    };
    assert_eq!(waiting.usage.tool_attempts, 2); // One completed read and one unentered reservation.
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(fixture.saved(&original).await.snapshot.usage, waiting.usage);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    let resumed = completed(
        agent
            .resume(fixture.approve(&original, "approve-late").await, context())
            .await
            .unwrap(),
    );
    let result = fixture.outcome(&resumed).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(result.usage.tool_attempts, 4);
    let target = fixture.tools[1].seen.lock().unwrap();
    assert_ne!(&target[0].context.attempt_id, attempt_id);
    assert_eq!(&target[0].context.idempotency_key, idempotency_key);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn repeated_commands_reuse_acceptance_after_completion_and_changed_decisions_conflict() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approval-command").await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    let before = fixture.saved(&resumed).await;
    let replay = completed(agent.resume(command.clone(), context()).await.unwrap());
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(fixture.saved(&replay).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    let mut conflicting = command.clone();
    let ResumeAction::Approve { wait_id, target } = command.action else {
        unreachable!()
    };
    conflicting.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "declined".into(),
    };
    assert_eq!(
        agent.resume(conflicting, context()).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
    let mut fresh = before.snapshot.resume_receipts[0].command.clone();
    fresh.command_id = id("new-command-after-terminal");
    fresh.expected_revision = before.snapshot.revision;
    assert!(agent.resume(fresh, context()).await.is_err());
    assert_eq!(fixture.saved(&replay).await.snapshot, before.snapshot);
}

#[tokio::test]
async fn explicit_system_input_changes_and_wrong_wait_binding_scope_or_revision_do_not_consume_commands()
 {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let saved = fixture.saved(&handle).await;
    let command = fixture.approve(&handle, "approval").await;
    for values in [
        JsonObject::new(),
        object(json!({"workspace_id":CHANGED_RECORD})),
    ] {
        let mut caller = context();
        caller.data.system_inputs = Some(SystemInputs::new(values));
        assert_eq!(
            agent
                .resume(command.clone(), caller)
                .await
                .unwrap_err()
                .code,
            ErrorCode::SystemInputsMismatch
        );
    }
    let mut wrong_scope = context();
    wrong_scope.data.scope.tenant_id = id("foreign");
    assert_eq!(
        agent
            .resume(command.clone(), wrong_scope)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    let mut stale = command.clone();
    stale.expected_revision -= 1;
    assert!(agent.resume(stale, context()).await.is_err());
    let mut wrong_wait = command.clone();
    if let ResumeAction::Approve { wait_id, .. } = &mut wrong_wait.action {
        *wait_id = id("other-wait");
    }
    assert!(agent.resume(wrong_wait, context()).await.is_err());
    let mut wrong_binding = command.clone();
    if let ResumeAction::Approve {
        target: ApprovalTarget::Tool { binding_digest, .. },
        ..
    } = &mut wrong_binding.action
    {
        *binding_digest = canonical_digest(&json!("other-target"));
    }
    assert!(agent.resume(wrong_binding, context()).await.is_err());
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    let mut same_values = context();
    same_values.data.system_inputs =
        Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
    let resumed = completed(agent.resume(command, same_values).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn a_recorded_approval_never_overrides_current_resume_or_target_denial() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let command = fixture.approve(&handle, "approve").await;
    fixture.policy.deny_resume.store(true, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
    fixture.policy.deny_resume.store(false, Ordering::SeqCst);
    fixture
        .policy
        .require_resume_approval
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        agent.resume(command.clone(), context()).await.unwrap(),
        Guarded::ApprovalRequired(_)
    ));
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
    fixture
        .policy
        .require_resume_approval
        .store(false, Ordering::SeqCst);
    fixture.policy.deny_execute.store(true, Ordering::SeqCst);
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    fixture.outcome(&resumed).await;
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)
    );
    fixture.policy.deny_resume.store(true, Ordering::SeqCst);
    assert_eq!(
        agent.resume(command, context()).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
}

#[tokio::test]
async fn denying_the_fixed_candidate_closes_only_that_call_and_preserves_prior_results() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let mut command = fixture.approve(&handle, "deny").await;
    let ResumeAction::Approve { wait_id, target } = command.action else {
        unreachable!()
    };
    command.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "Target declined by reviewer".into(),
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before", "after"]);
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)
    );
    assert_eq!(fixture.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn competing_identical_commands_consume_once_even_when_an_earlier_caller_is_paused() {
    let fixture = Fixture::new(Mode::Approval);
    let first = fixture.agent();
    let second = fixture.agent();
    let original = fixture.started(&first).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "shared-approval").await;
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let delayed = tokio::spawn({
        let command = command.clone();
        async move { first.resume(command, context()).await }
    });
    gate(&fixture.policy.resume_entered).await;
    let winner = completed(second.resume(command.clone(), context()).await.unwrap());
    let result = fixture.outcome(&winner).await;
    fixture.policy.resume_release.add_permits(1);
    let replay = completed(delayed.await.unwrap().unwrap());
    assert_eq!(fixture.outcome(&replay).await, result);
    let saved = fixture.saved(&replay).await;
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_resumed_observers_does_not_cancel_the_accepted_execution() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.model.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "resume-owned").await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    gate(&fixture.model.final_entered).await;
    let observer = context();
    let mut outcome_future = Box::pin(resumed.outcome(&observer));
    assert!(futures_util::poll!(outcome_future.as_mut()).is_pending());
    drop(outcome_future);
    let mut events = resumed.events(0, context());
    events.next().await.unwrap().unwrap();
    drop(events);
    drop(resumed);
    fixture.model.final_release.add_permits(1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.outcome(&original).await, waiting);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn input_answers_use_the_saved_output_schema_and_do_not_reenter_the_question_tool() {
    let fixture = Fixture::new(Mode::Input);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let before = fixture.saved(&original).await;
    let wait = before.snapshot.wait.as_ref().unwrap();
    assert!(matches!(wait.target, WaitTarget::Input { .. }));
    let mut command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("answer"),
        action: ResumeAction::Input {
            wait_id: wait.wait_id.clone(),
            answer: json!({"selection":"annual"}),
        },
    };
    for answer in [
        json!(42),
        json!({}),
        json!({"selection":"unknown"}),
        json!({"selection":"annual","workspace_id":WORKSPACE}),
    ] {
        let mut invalid = command.clone();
        if let ResumeAction::Input { answer: value, .. } = &mut invalid.action {
            *value = answer;
        }
        assert!(agent.resume(invalid, context()).await.is_err());
        assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    }
    command.command_id = id("valid-answer");
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let result = fixture.outcome(&resumed).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["before", "target", "after"]
    );
    let saved = fixture.saved(&resumed).await;
    assert!(
        matches!(&saved.snapshot.tool_ledger[1].state, ToolCallState::Settled { result } if result.effect == ToolEffect::NotApplied && result.content == vec![InputContent::Json { value: json!({"selection":"annual"}) }])
    );
    let requests = fixture.model.requests.lock().unwrap();
    let target_results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } if provider_call_id == &id("provider-1") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(
        target_results,
        vec![
            &json!({"status":"succeeded","effect":"not_applied","content":[{"type":"json","value":{"selection":"annual"}}]})
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn waiting_time_counts_toward_the_original_run_deadline_without_new_dispatch() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "expired-approval").await;
    tokio::time::advance(Duration::from_millis(101)).await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let result = fixture.outcome(&resumed).await;
    assert_eq!(
        result.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert_eq!(result.usage.model_calls, waiting.usage.model_calls);
    assert_eq!(result.usage.tool_attempts, waiting.usage.tool_attempts);
    assert!(result.usage.elapsed_ms >= 100);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_a_saved_wait_without_a_local_driver_prevents_new_resume_commands() {
    let fixture = Fixture::new(Mode::Approval);
    let original_agent = fixture.agent();
    let original = fixture.started(&original_agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approve-too-late").await;
    drop(original_agent);
    let reopened = fixture.agent();
    let mut execution = context();
    execution.data.system_inputs =
        Some(SystemInputs::new(object(json!({"workspace_id":WORKSPACE}))));
    let handle = completed(reopened.start(request("request"), execution).await.unwrap());
    fixture.policy.deny_cancel.store(true, Ordering::SeqCst);
    assert_eq!(
        handle
            .cancel(id("not-authorized"), &context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture.saved(&handle).await.snapshot.status,
        RunStatus::Waiting
    );
    fixture.policy.deny_cancel.store(false, Ordering::SeqCst);
    let receipt = completed(handle.cancel(id("stop-waiting"), &context()).await.unwrap());
    assert_ne!(receipt, CancelReceipt::NotLocal);
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Cancelled);
    assert!(reopened.resume(command, context()).await.is_err());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_command_commit_leaves_the_wait_intact_and_a_retry_executes_once() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let before = fixture.saved(&original).await;
    let command = fixture.approve(&original, "retry-commit").await;
    fixture.store.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&resumed).await.snapshot.resume_receipts.len(),
        1
    );
}

#[tokio::test]
async fn command_commit_ack_loss_is_recovered_without_consuming_or_executing_twice() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "lost-ack").await;
    fixture.store.mode.store(2, Ordering::SeqCst);
    let initial = agent.resume(command.clone(), context()).await;
    if let Err(error) = initial {
        assert_eq!(error.code, ErrorCode::PersistenceUnavailable);
    }
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.store.consumed_commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&replay).await.snapshot.resume_receipts.len(),
        1
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_a_polled_resume_future_does_not_abandon_its_command_transaction() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "detached-resume").await;
    fixture.store.mode.store(3, Ordering::SeqCst);
    let mut resuming = Box::pin(agent.resume(command.clone(), context()));
    assert!(futures_util::poll!(resuming.as_mut()).is_pending());
    gate(&fixture.store.entered).await;
    drop(resuming);
    fixture.store.release.add_permits(1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&replay).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.saved(&replay).await.snapshot.resume_receipts.len(),
        1
    );
}

#[tokio::test]
async fn a_cancelled_wait_rejects_an_approval_still_paused_at_its_current_policy_check() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "losing-approval").await;
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let pending = tokio::spawn(async move { agent.resume(command, context()).await });
    gate(&fixture.policy.resume_entered).await;
    completed(
        original
            .cancel(id("cancel-before-approval"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Cancelled
    );
    fixture.policy.resume_release.add_permits(1);
    assert!(pending.await.unwrap().is_err());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .saved(&original)
            .await
            .snapshot
            .resume_receipts
            .is_empty()
    );
}

async fn external_command(
    fixture: &Fixture,
    handle: &RunHandle,
    reference: RecordRef,
) -> ResumeCommand {
    let saved = fixture.saved(handle).await;
    let wait = saved.snapshot.wait.unwrap();
    assert!(matches!(wait.target, WaitTarget::External { .. }));
    ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("external-result"),
        action: ResumeAction::External {
            wait_id: wait.wait_id,
            receipt_ref: reference,
        },
    }
}

#[tokio::test]
async fn external_receipts_require_current_access_and_host_verification_before_effect_resolution() {
    let fixture = Fixture::new(Mode::External);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    let saved = fixture.saved(&original).await;
    let valid =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    fixture
        .policy
        .deny_receipt_read
        .store(true, Ordering::SeqCst);
    assert_eq!(
        agent
            .resume(valid.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
    fixture
        .policy
        .deny_receipt_read
        .store(false, Ordering::SeqCst);
    let mut wrong_ref = fixture.store.proof.reference().clone();
    wrong_ref.digest = canonical_digest(&json!("wrong-record"));
    assert!(
        agent
            .resume(
                external_command(&fixture, &original, wrong_ref).await,
                context()
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
    let forged = external_command(
        &fixture,
        &original,
        fixture.store.forged.reference().clone(),
    )
    .await;
    assert_eq!(
        agent.resume(forged, context()).await.unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.saved(&original).await.snapshot, saved.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let mut reviewer = context();
    reviewer.data.principal_ref = id("effect-reviewer");
    let resumed = completed(agent.resume(valid.clone(), reviewer).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(outcome.unresolved_effects.is_empty());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.outcome(&original).await, waiting);
    let requests = fixture.model.requests.lock().unwrap();
    let target_results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } if provider_call_id == &id("provider-1") => Some(content),
            _ => None,
        })
        .collect();
    assert_eq!(
        target_results,
        vec![
            &json!({"status":"succeeded","effect":"applied","content":[{"type":"json","value":"verified target"}]})
        ]
    );
}

#[tokio::test]
async fn an_external_verdict_that_remains_unknown_keeps_the_wait_and_command_unconsumed() {
    let fixture = Fixture::new(Mode::External);
    fixture.verifier.mode.store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let before = fixture.saved(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ToolEffectUnresolved
    );
    assert_eq!(fixture.saved(&original).await.snapshot, before.snapshot);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    fixture.verifier.mode.store(0, Ordering::SeqCst);
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_verified_write_with_invalid_output_keeps_the_applied_effect_and_original_receipt() {
    let fixture = Fixture::new(Mode::External);
    fixture.verifier.mode.store(2, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    fixture.outcome(&resumed).await;
    let saved = fixture.saved(&resumed).await;
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[1].state else {
        panic!("expected reconciled effect")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.effect, ToolEffect::Applied);
    let receipt = fixture
        .base
        .store
        .read_record(&scope(), result.effect_receipt_ref.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(&receipt.value()["receipt"], fixture.store.proof.value());
    let replay = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&replay).await;
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_competing_denial_consumes_the_wait_before_a_paused_approval() {
    let fixture = Fixture::new(Mode::Approval);
    let first = fixture.agent();
    let second = fixture.agent();
    let original = fixture.started(&first).await;
    fixture.outcome(&original).await;
    let approval = fixture.approve(&original, "losing-approval").await;
    let mut denial = approval.clone();
    denial.command_id = id("winning-denial");
    let ResumeAction::Approve { wait_id, target } = denial.action else {
        unreachable!()
    };
    denial.action = ResumeAction::Deny {
        wait_id,
        target,
        reason: "Do not apply".into(),
    };
    fixture
        .policy
        .pause_next_resume
        .store(true, Ordering::SeqCst);
    let delayed = tokio::spawn(async move { first.resume(approval, context()).await });
    gate(&fixture.policy.resume_entered).await;
    let winner = completed(second.resume(denial.clone(), context()).await.unwrap());
    fixture.outcome(&winner).await;
    fixture.policy.resume_release.add_permits(1);
    assert!(delayed.await.unwrap().is_err());
    let saved = fixture.saved(&winner).await;
    assert_eq!(saved.snapshot.resume_receipts.len(), 1);
    assert_eq!(saved.snapshot.resume_receipts[0].command, denial);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(*fixture.order.lock().unwrap(), vec!["before", "after"]);
}

#[tokio::test]
async fn resume_rejects_replacement_profiles_and_changed_answer_contracts() {
    let fixture = Fixture::new(Mode::Input);
    let original_agent = fixture.agent();
    let handle = fixture.started(&original_agent).await;
    fixture.outcome(&handle).await;
    let saved = fixture.saved(&handle).await;
    let wait = saved.snapshot.wait.as_ref().unwrap();
    let mut command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("answer-pinned"),
        action: ResumeAction::Input {
            wait_id: wait.wait_id.clone(),
            answer: json!(42),
        },
    };
    let mut changed_profile = fixture.profile.clone();
    changed_profile.version = id("2");
    let changed_agent = create_agent(changed_profile, fixture.bindings()).unwrap();
    let mut valid_answer = command.clone();
    if let ResumeAction::Input { answer, .. } = &mut valid_answer.action {
        *answer = json!({"selection":"annual"});
    }
    assert!(changed_agent.resume(valid_answer, context()).await.is_err());
    let mut bindings = fixture.bindings();
    let mut registrations = vec![];
    for name in ["before", "target", "after"] {
        let existing = fixture.registry.get(&id(name)).unwrap();
        let mut descriptor = existing.compiled.descriptor().clone();
        if name == "target" {
            descriptor.output_schema = json!({"type":"integer"});
        }
        registrations.push(ToolRegistration {
            compiled: SchemaCompiler::new()
                .compile(descriptor, &fixture.inputs)
                .unwrap(),
            executor: existing.executor.clone(),
        });
    }
    bindings.tools = Some(std::sync::Arc::new(
        ToolRegistry::new(scope(), registrations).unwrap(),
    ));
    let changed_contract = create_agent(fixture.profile.clone(), bindings).unwrap();
    assert!(
        changed_contract
            .resume(command.clone(), context())
            .await
            .is_err()
    );
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    if let ResumeAction::Input { answer, .. } = &mut command.action {
        *answer = json!({"selection":"quarterly"});
    }
    let resumed = completed(original_agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn expiry_and_cancellation_of_an_external_wait_preserve_the_uncertain_effect_without_verifying_it()
 {
    for expire in [false, true] {
        let mut fixture = Fixture::new(Mode::External);
        fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
        let agent = fixture.agent();
        let original = fixture.started(&agent).await;
        let waiting = fixture.outcome(&original).await;
        assert!(!waiting.unresolved_effects.is_empty());
        let outcome = if expire {
            let command =
                external_command(&fixture, &original, fixture.store.proof.reference().clone())
                    .await;
            tokio::time::advance(Duration::from_millis(101)).await;
            let resumed = completed(agent.resume(command, context()).await.unwrap());
            fixture.outcome(&resumed).await
        } else {
            completed(
                original
                    .cancel(id("stop-external-wait"), &context())
                    .await
                    .unwrap(),
            );
            fixture.outcome(&original).await
        };
        assert_eq!(
            outcome.result.status(),
            if expire {
                RunStatus::Exhausted
            } else {
                RunStatus::Cancelled
            }
        );
        assert_eq!(outcome.unresolved_effects, waiting.unresolved_effects);
        assert_eq!(outcome.usage.tool_attempts, waiting.usage.tool_attempts);
        assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_verified_not_applied_result_settles_the_call_without_retrying_the_business_operation() {
    let fixture = Fixture::new(Mode::External);
    fixture.tools[1].apply_write.store(false, Ordering::SeqCst);
    fixture.verifier.mode.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command =
        external_command(&fixture, &original, fixture.store.proof.reference().clone()).await;
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(outcome.unresolved_effects.is_empty());
    let saved = fixture.saved(&resumed).await;
    assert!(matches!(&saved.snapshot.tool_ledger[1].state,
        ToolCallState::Settled { result } if result.status == ToolResultStatus::Failed && result.effect == ToolEffect::NotApplied));
    let replay = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&replay).await;
    assert_eq!(fixture.verifier.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn an_expired_individual_wait_cannot_execute_even_while_the_run_has_time_remaining() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.store.wait_timeout_ms.store(20, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let saved = fixture.saved(&original).await;
    let expires = saved.snapshot.wait.as_ref().unwrap().expires_at_ms.unwrap();
    assert!(expires < saved.snapshot.timing.deadline_at_ms);
    let command = fixture.approve(&original, "expired-short-wait").await;
    tokio::time::advance(Duration::from_millis(30)).await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert!(outcome.usage.elapsed_ms < fixture.profile.limits.max_elapsed_ms.get());
    assert_eq!(outcome.usage.tool_attempts, waiting.usage.tool_attempts);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.saved(&resumed).await.snapshot.resume_receipts[0].expired);
}

#[tokio::test(start_paused = true)]
async fn an_approval_accepted_before_wait_expiry_keeps_running_after_that_old_deadline() {
    let fixture = Fixture::new(Mode::Approval);
    fixture.store.wait_timeout_ms.store(100, Ordering::SeqCst);
    fixture.model.hold_final.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let waiting = fixture.outcome(&original).await;
    let command = fixture.approve(&original, "accepted-before-expiry").await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    gate(&fixture.model.final_entered).await;
    assert!(!fixture.saved(&resumed).await.snapshot.resume_receipts[0].expired);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(150)).await;
    fixture.model.final_release.add_permits(1);
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.outcome(&original).await, waiting);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn historical_segment_outcomes_recheck_permission_after_the_protected_record_read() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    fixture.outcome(&original).await;
    let command = fixture.approve(&original, "approval").await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    fixture.outcome(&resumed).await;
    let saved = fixture.saved(&resumed).await;
    let previous_ref = saved.snapshot.resume_receipts[0]
        .previous_outcome_ref
        .clone();
    *fixture.store.pause_record.lock().unwrap() = Some(previous_ref);
    let checks_before = fixture.policy.details_checks.load(Ordering::SeqCst);
    let observer = tokio::spawn(async move { original.outcome(&context()).await });
    gate(&fixture.store.record_entered).await;
    // The first snapshot permission was granted; only the subsequent protected read is suspended.
    assert!(fixture.policy.details_checks.load(Ordering::SeqCst) > checks_before);
    fixture.policy.deny_details.store(true, Ordering::SeqCst);
    fixture.store.record_release.add_permits(1);
    assert_eq!(
        observer.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn recovery_query_budget_exhaustion_keeps_the_applied_write_unknown_without_redispatch() {
    let mut fixture = Fixture::new(Mode::External);
    fixture.profile.limits.max_recovery_attempts = 1;
    fixture.store.mode.store(4, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        original.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let before = fixture.saved(&original).await;
    assert_eq!(before.snapshot.status, RunStatus::Running);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert!(matches!(
        before.snapshot.tool_ledger[1].state,
        ToolCallState::Dispatching { .. }
    ));
    fixture.store.mode.store(0, Ordering::SeqCst);
    let source = before
        .snapshot
        .recovery_record(id("write-checkpoint"))
        .unwrap();
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("recover-write"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let resumed = completed(fixture.agent().resume(command, context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert!(matches!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::RecoveryAttempts
        }
    ));
    assert_eq!(outcome.unresolved_effects.len(), 1);
    let record = fixture
        .base
        .store
        .read_record(&scope(), &outcome.unresolved_effects[0])
        .await
        .unwrap();
    let result: ToolResult = serde_json::from_value(record.value().clone()).unwrap();
    assert_eq!(result.effect, ToolEffect::Unknown);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let after = fixture.saved(&resumed).await;
    assert!(matches!(
        after.snapshot.tool_ledger[1].state,
        ToolCallState::Unknown { .. }
    ));
    assert_eq!(
        after.snapshot.tool_ledger[1].call.bound_input_ref,
        before.snapshot.tool_ledger[1].call.bound_input_ref
    );
}

#[tokio::test]
async fn recovered_unknown_write_can_resume_with_a_verified_receipt_and_preserved_segment_history()
{
    let mut fixture = Fixture::new(Mode::External);
    fixture.profile.limits.max_recovery_attempts = 4;
    fixture.store.mode.store(4, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        original.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let before = fixture.saved(&original).await;
    fixture.store.mode.store(0, Ordering::SeqCst);
    let source = before
        .snapshot
        .recovery_record(id("interrupted-write"))
        .unwrap();
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("recover-write"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let recovered = completed(fixture.agent().resume(command, context()).await.unwrap());
    let waiting = fixture.outcome(&recovered).await;
    assert_eq!(waiting.result.status(), RunStatus::Waiting);
    assert_eq!(waiting.unresolved_effects.len(), 1);
    let receipt = external_command(
        &fixture,
        &recovered,
        fixture.store.proof.reference().clone(),
    )
    .await;
    let resumed = completed(fixture.agent().resume(receipt, context()).await.unwrap());
    let outcome = fixture.outcome(&resumed).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert!(outcome.unresolved_effects.is_empty());
    assert_eq!(fixture.outcome(&recovered).await, waiting);
    let after = fixture.saved(&resumed).await;
    assert_eq!(after.snapshot.resume_receipts.len(), 1);
    assert_eq!(after.snapshot.recovery_receipts.len(), 1);
    assert_eq!(
        after.snapshot.resume_receipts[0].previous_segment_start_revision,
        after.snapshot.recovery_receipts[0].accepted_revision
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), 1);
    let checkpoint = fixture.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_recovery_preserves_an_unconfirmed_write_without_querying_or_dispatching() {
    let fixture = Fixture::new(Mode::External);
    fixture.store.mode.store(4, Ordering::SeqCst);
    let original = fixture.started(&fixture.agent()).await;
    assert_eq!(
        original.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let before = fixture.saved(&original).await;
    fixture.store.mode.store(0, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(11)).await;
    let source = before
        .snapshot
        .recovery_record(id("expired-write"))
        .unwrap();
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("recover-expired-write"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let recovered = completed(fixture.agent().resume(command, context()).await.unwrap());
    let outcome = fixture.outcome(&recovered).await;
    assert!(matches!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    assert_eq!(outcome.unresolved_effects.len(), 1);
    assert_eq!(outcome.usage.recovery_attempts, 0);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture.saved(&recovered).await;
    assert!(matches!(
        saved.snapshot.tool_ledger[1].state,
        ToolCallState::Unknown { .. }
    ));
    let result: ToolResult = serde_json::from_value(
        fixture
            .base
            .store
            .read_record(&scope(), &outcome.unresolved_effects[0])
            .await
            .unwrap()
            .value()
            .clone(),
    )
    .unwrap();
    assert_eq!(result.effect, ToolEffect::Unknown);
}

#[tokio::test]
async fn persistent_storage_failure_reports_the_last_confirmed_revision_and_uncertain_effect_under_current_permission()
 {
    let fixture = Fixture::new(Mode::External);
    fixture.store.mode.store(5, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    let error = original.outcome(&context()).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::PersistenceUnavailable);
    let diagnostic = error
        .persistence
        .expect("authorized recovery metadata survives storage outage");
    let saved = fixture.saved(&original).await;
    assert_eq!(diagnostic.run_id, *original.run_id());
    assert_eq!(diagnostic.last_confirmed_revision, saved.snapshot.revision);
    assert_eq!(saved.snapshot.status, RunStatus::Running);
    assert!(saved.snapshot.outcome.is_none());
    assert_eq!(diagnostic.unconfirmed_effects.len(), 1);
    let effect = &diagnostic.unconfirmed_effects[0];
    let ToolCallState::Dispatching {
        attempt_id,
        idempotency_key,
    } = &saved.snapshot.tool_ledger[1].state
    else {
        panic!("last confirmed state precedes settlement");
    };
    assert_eq!(&effect.attempt_id, attempt_id);
    assert_eq!(&effect.idempotency_key, idempotency_key);
    assert_eq!(
        effect.bound_input_ref,
        saved.snapshot.tool_ledger[1].call.bound_input_ref
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let events = fixture
        .base
        .store
        .read_events(&scope(), original.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(
        !events
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    fixture.policy.deny_details.store(true, Ordering::SeqCst);
    let denied = original.outcome(&context()).await.unwrap_err();
    assert_eq!(denied.code, ErrorCode::AccessDenied);
    assert!(denied.persistence.is_none());
}
