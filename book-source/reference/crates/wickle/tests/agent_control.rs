//! Durable controls settle owned intervals without replaying business operations.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use wickle::*;

fn command(name: &str, action: ControlAction) -> ControlCommand {
    ControlCommand {
        command_id: id(name),
        principal_ref: context().data.principal_ref,
        action,
    }
}

#[tokio::test]
async fn another_agent_can_submit_a_durable_cancel_that_the_owning_driver_consumes() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let owner = fixture.agent();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let cancel = command(
        "remote-cancel",
        ControlAction::Cancel {
            reason: id("shutdown"),
        },
    );
    let receipt = completed(
        remote
            .submit_control_command(handle.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(matches!(outcome.result, OutcomeResult::Cancelled { .. }));
    let history = fixture
        .store
        .read_execution(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(history.controls.len(), 1);
    assert_eq!(
        history.controls[0].processed_segment_id,
        Some(history.segments[0].segment_id.clone())
    );
    let repeated = completed(
        remote
            .submit_control_command(handle.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert_eq!(
        repeated.processed_segment_id,
        history.controls[0].processed_segment_id
    );
    assert_eq!(
        fixture
            .store
            .read_execution(&scope(), handle.run_id())
            .await
            .unwrap(),
        history
    );
    let changed = ControlCommand {
        action: ControlAction::Cancel {
            reason: id("changed"),
        },
        ..cancel
    };
    assert_eq!(
        remote
            .submit_control_command(handle.run_id().clone(), changed, context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_cancel_has_its_own_segment_and_old_handle_and_queries_do_not_change_it() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    let cancel = command(
        "idle-cancel",
        ControlAction::Cancel {
            reason: id("withdrawn"),
        },
    );
    let receipt = completed(
        agent
            .submit_control_command(old.run_id().clone(), cancel.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(fixture.outcome(&old).await, waiting);
    let history = fixture
        .base
        .store
        .read_execution(&scope(), old.run_id())
        .await
        .unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(
        history.segments[1].segment_id,
        receipt.processed_segment_id.clone().unwrap()
    );
    assert_ne!(old.segment_id(), &history.segments[1].segment_id);
    let duplicate = fixture.started(&agent).await;
    assert_eq!(duplicate.segment_id(), &history.segments[1].segment_id);
    assert_eq!(
        fixture.outcome(&duplicate).await.result.status(),
        RunStatus::Cancelled
    );
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    assert_eq!(
        completed(
            agent
                .get_control_receipt(old.run_id(), &receipt.command_id, &context())
                .await
                .unwrap()
        ),
        receipt
    );
    for _ in 0..3 {
        assert_eq!(
            completed(agent.get_run(old.run_id(), &context()).await.unwrap()).status,
            RunStatus::Cancelled
        );
    }
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    let repeated = completed(
        agent
            .submit_control_command(old.run_id().clone(), cancel, context())
            .await
            .unwrap(),
    );
    assert_eq!(
        repeated.processed_segment_id,
        Some(history.segments[1].segment_id.clone())
    );
    assert_eq!(
        fixture
            .base
            .store
            .read_execution(&scope(), old.run_id())
            .await
            .unwrap(),
        history
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn explicit_idle_expiry_preserves_the_old_wait_and_dispatches_nothing() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    let expire = command("expire", ControlAction::Expire);
    let receipt = completed(
        agent
            .submit_control_command(old.run_id().clone(), expire.clone(), context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    tokio::time::advance(Duration::from_millis(101)).await;
    let finished = completed(
        agent
            .process_control_command(old.run_id().clone(), expire.command_id, context())
            .await
            .unwrap(),
    );
    assert!(finished.processed_segment_id.is_some());
    assert_eq!(fixture.outcome(&old).await, waiting);
    let latest = fixture.saved(&old).await;
    assert!(matches!(
        latest.snapshot.outcome.unwrap().result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    assert!(latest.session.active_run_id.is_none());
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_an_interrupted_run_preserves_the_interrupted_handle() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let agent = fixture.agent();
    let old = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        old.stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    let interrupted = completed(old.outcome(&context()).await.unwrap());
    assert_eq!(interrupted.result.status(), RunStatus::Interrupted);
    let receipt = completed(
        old.cancel(id("cancel-interrupted"), &context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(
        completed(old.outcome(&context()).await.unwrap()),
        interrupted
    );
    assert_eq!(
        completed(agent.get_run(old.run_id(), &context()).await.unwrap()).status,
        RunStatus::Cancelled
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn competing_idle_cancel_and_expire_settle_once_and_record_both_receipts() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    let waiting = fixture.outcome(&old).await;
    tokio::time::advance(Duration::from_millis(101)).await;
    let (cancel, expire) = tokio::join!(
        agent.submit_control_command(
            old.run_id().clone(),
            command(
                "cancel",
                ControlAction::Cancel {
                    reason: id("withdrawn")
                }
            ),
            context()
        ),
        agent.submit_control_command(
            old.run_id().clone(),
            command("expire", ControlAction::Expire),
            context()
        ),
    );
    assert!(completed(cancel.unwrap()).processed_segment_id.is_some());
    assert!(completed(expire.unwrap()).processed_segment_id.is_some());
    let history = fixture
        .base
        .store
        .read_execution(&scope(), old.run_id())
        .await
        .unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(history.controls.len(), 2);
    assert_eq!(fixture.outcome(&old).await, waiting);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn checkpoint_import_rejects_forged_historical_outcomes_and_event_boundaries() {
    let fixture = Fixture::new(Mode::Approval);
    let agent = fixture.agent();
    let old = fixture.started(&agent).await;
    fixture.outcome(&old).await;
    completed(old.cancel(id("cancel"), &context()).await.unwrap());
    let checkpoint = fixture.base.store.export_checkpoint(&scope()).unwrap();
    let original = serde_json::to_value(&checkpoint).unwrap();
    StateStoreCheckpoint::from_json(&original.to_string(), &scope(), &checkpoint.digest()).unwrap();
    for mutation in ["outcome", "boundary", "missing_boundary"] {
        let mut image = original.clone();
        let segment = &mut image["executions"][0]["segments"][0];
        match mutation {
            "outcome" => {
                segment["outcome"]["outcome"]["output"] =
                    serde_json::to_value(vec![InputContent::Text {
                        text: "forged previous answer".into(),
                    }])
                    .unwrap()
            }
            "boundary" => segment["last_event_seq"] = serde_json::json!(1),
            "missing_boundary" => {
                segment.as_object_mut().unwrap().remove("last_event_seq");
            }
            _ => unreachable!(),
        }
        assert!(
            StateStoreCheckpoint::from_json(
                &image.to_string(),
                &scope(),
                &canonical_digest(&image)
            )
            .is_err(),
            "accepted {mutation}"
        );
    }
}

#[tokio::test]
async fn remote_stop_is_consumed_as_a_recoverable_interruption() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let owner = fixture.agent();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let stop = command(
        "remote-stop",
        ControlAction::Stop {
            cause: InterruptionCause::HostShutdown,
        },
    );
    let receipt = completed(
        remote
            .submit_control_command(handle.run_id().clone(), stop, context())
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    let outcome = completed(
        tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(
        matches!(outcome.result, OutcomeResult::Interrupted { interruption } if interruption.cause == InterruptionCause::HostShutdown)
    );
    let processed = completed(
        remote
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        processed.processed_segment_id.as_ref(),
        Some(handle.segment_id())
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

struct StopPolicy(InterruptionAction);
impl InterruptionPolicy for StopPolicy {
    fn identity(&self) -> VersionedRef {
        reference("stop-policy")
    }
    fn decide<'a>(&'a self, _: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision> {
        Box::pin(async move {
            Ok(InterruptionDecision {
                action: self.0,
                app_state: None,
            })
        })
    }
}
#[tokio::test]
async fn durable_stop_records_custom_pause_cancel_and_fail_decisions() {
    for (action, expected) in [
        (InterruptionAction::Pause, RunStatus::Interrupted),
        (InterruptionAction::Cancel, RunStatus::Cancelled),
        (InterruptionAction::Fail, RunStatus::Failed),
    ] {
        let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(InterruptionPolicyBinding {
            policy: std::sync::Arc::new(StopPolicy(action)),
            configuration: JsonObject::new(),
            app_state_schema: None,
            timeout_ms: 1000.try_into().unwrap(),
        });
        let owner = create_agent(agent_support::profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "request").await;
        fixture.model.entered.notified().await;
        let remote = fixture.agent();
        let receipt = completed(
            remote
                .submit_control_command(
                    handle.run_id().clone(),
                    command(
                        "stop",
                        ControlAction::Stop {
                            cause: InterruptionCause::SegmentStopped,
                        },
                    ),
                    context(),
                )
                .await
                .unwrap(),
        );
        let outcome = completed(
            tokio::time::timeout(Duration::from_secs(3), handle.outcome(&context()))
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(outcome.result.status(), expected);
        let completed = completed(
            remote
                .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
                .await
                .unwrap(),
        );
        assert_eq!(
            completed.processed_segment_id.as_ref(),
            Some(handle.segment_id())
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn active_expiry_is_pending_before_the_deadline_and_consumed_when_the_budget_ends() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let mut profile = agent_support::profile();
    profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let owner = create_agent(profile, fixture.bindings()).unwrap();
    let handle = fixture.started(&owner, "request").await;
    fixture.model.entered.notified().await;
    let remote = fixture.agent();
    let receipt = completed(
        remote
            .submit_control_command(
                handle.run_id().clone(),
                command("expire", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    assert!(receipt.processed_segment_id.is_none());
    assert!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome
            .is_none()
    );
    tokio::time::advance(Duration::from_millis(101)).await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert!(matches!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    let processed = completed(
        remote
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        processed.processed_segment_id.as_ref(),
        Some(handle.segment_id())
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn denied_worker_processing_and_receipt_reads_leave_the_pending_command_unchanged() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    let receipt = completed(
        agent
            .submit_control_command(
                handle.run_id().clone(),
                command("expire", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    tokio::time::advance(Duration::from_millis(101)).await;
    fixture
        .policy
        .deny_control_processing
        .store(true, Ordering::SeqCst);
    fixture.policy.deny_details.store(true, Ordering::SeqCst);
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    assert_eq!(
        agent
            .process_control_command(
                handle.run_id().clone(),
                receipt.command_id.clone(),
                context()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        agent
            .get_control_receipt(handle.run_id(), &receipt.command_id, &context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    fixture
        .policy
        .deny_control_processing
        .store(false, Ordering::SeqCst);
    fixture.policy.deny_details.store(false, Ordering::SeqCst);
    assert!(
        completed(
            agent
                .process_control_command(handle.run_id().clone(), receipt.command_id, context())
                .await
                .unwrap()
        )
        .processed_segment_id
        .is_some()
    );
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn idle_deadline_observation_does_not_settle_waits_or_change_saved_state() {
    let mut fixture = Fixture::new(Mode::Approval);
    fixture.profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let waiting = fixture.outcome(&handle).await;
    let before = fixture.base.store.export_checkpoint(&scope()).unwrap();
    let usage = fixture.saved(&handle).await.snapshot.usage;
    assert!(!completed(agent.get_run(handle.run_id(), &context()).await.unwrap()).deadline_expired);
    let saved = fixture.saved(&handle).await;
    let remaining =
        (saved.snapshot.timing.deadline_at_ms - fixture.base.clock.now().unwrap().utc_ms) as u64;
    tokio::time::advance(Duration::from_millis(remaining - 1)).await;
    assert!(!completed(agent.get_run(handle.run_id(), &context()).await.unwrap()).deadline_expired);
    tokio::time::advance(Duration::from_millis(1)).await;
    for _ in 0..2 {
        let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
        assert_eq!(view.status, RunStatus::Waiting);
        assert!(view.deadline_expired);
        assert_eq!(view.usage, usage);
    }
    assert_eq!(
        fixture
            .base
            .store
            .export_checkpoint(&scope())
            .unwrap()
            .digest(),
        before.digest()
    );
    assert_eq!(fixture.outcome(&handle).await, waiting);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
    completed(
        agent
            .submit_control_command(
                handle.run_id().clone(),
                command("expire-observed", ControlAction::Expire),
                context(),
            )
            .await
            .unwrap(),
    );
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Exhausted);
    assert!(!view.deadline_expired);
}

#[tokio::test(start_paused = true)]
async fn interrupted_deadline_observation_is_read_only_at_the_original_deadline() {
    let fixture = agent_support::Fixture::new(agent_support::Response::Text, true);
    let mut profile = agent_support::profile();
    profile.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = create_agent(profile, fixture.bindings()).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    let interrupted = completed(handle.outcome(&context()).await.unwrap());
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let remaining =
        (saved.snapshot.timing.deadline_at_ms - fixture.clock.now().unwrap().utc_ms) as u64;
    tokio::time::advance(Duration::from_millis(remaining)).await;
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Interrupted);
    assert!(view.deadline_expired);
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap()),
        interrupted
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
