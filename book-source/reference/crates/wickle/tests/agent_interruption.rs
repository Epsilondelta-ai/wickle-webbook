//! Execution stops preserve checkpoints and cannot override protected core causes.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::Notify;
use wickle::*;

#[derive(Clone, Copy)]
enum Behavior {
    Pause,
    Cancel,
    Fail,
    Pending,
    Panic,
    Error,
    InvalidState,
    DefaultState,
}
struct AppPolicy {
    behavior: Behavior,
    calls: AtomicUsize,
    entered: Notify,
}
impl InterruptionPolicy for AppPolicy {
    fn identity(&self) -> VersionedRef {
        reference("maintenance")
    }
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            assert_eq!(info.configuration.get("mode"), Some(&json!("safe")));
            assert_eq!(info.scope, scope());
            match self.behavior {
                Behavior::Pending => std::future::pending().await,
                Behavior::Panic => panic!("private callback panic"),
                Behavior::Error => Err(ContractError::new(
                    ErrorCode::InvalidContract,
                    "private callback error",
                )),
                behavior => Ok(InterruptionDecision {
                    action: match behavior {
                        Behavior::Cancel => InterruptionAction::Cancel,
                        Behavior::Fail => InterruptionAction::Fail,
                        Behavior::DefaultState => InterruptionAction::UseDefault,
                        _ => InterruptionAction::Pause,
                    },
                    app_state: Some(AppState {
                        namespace: id("operations"),
                        status: id("maintenance_hold"),
                        metadata: [(
                            "reason".into(),
                            if matches!(behavior, Behavior::InvalidState) {
                                json!(42)
                            } else {
                                json!("host maintenance")
                            },
                        )]
                        .into(),
                    }),
                }),
            }
        })
    }
}
fn policy(behavior: Behavior) -> (Arc<AppPolicy>, InterruptionPolicyBinding) {
    let policy = Arc::new(AppPolicy {
        behavior,
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
    });
    let binding = InterruptionPolicyBinding {
        policy: policy.clone(),
        configuration: [("mode".into(), json!("safe"))].into(),
        app_state_schema: Some(AppStateSchema {
            namespace: id("operations"),
            schema: json!({"type":"object","properties":{
            "namespace":{"const":"operations"},"status":{"enum":["maintenance_hold"]},"metadata":{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"],"additionalProperties":false}
        },"required":["namespace","status","metadata"],"additionalProperties":false}),
        }),
        timeout_ms: 1000.try_into().unwrap(),
    };
    (policy, binding)
}
async fn outcome(handle: &RunHandle) -> RunOutcome {
    completed(
        tokio::time::timeout(Duration::from_secs(12), handle.outcome(&context()))
            .await
            .unwrap()
            .unwrap(),
    )
}
#[tokio::test]
async fn default_stop_records_a_nonterminal_checkpoint_and_does_not_restart_the_model() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        completed(
            handle
                .stop_execution(InterruptionCause::SegmentStopped, &context())
                .await
                .unwrap()
        ),
        ExecutionStopReceipt::Requested
    );
    let result = outcome(&handle).await;
    let OutcomeResult::Interrupted { interruption } = &result.result else {
        panic!("expected interruption: {result:?}")
    };
    assert_eq!(interruption.cause, InterruptionCause::SegmentStopped);
    assert!(interruption.recoverable);
    assert_eq!(result.app_state, None);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.session.active_run_id.as_ref(), Some(handle.run_id()));
    assert_eq!(saved.snapshot.interruption_records.len(), 1);
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let replay = fixture.started(&agent, "request").await;
    assert_eq!(outcome(&replay).await, result);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn app_policy_can_pause_cancel_or_fail_but_business_state_does_not_define_core_status() {
    for (behavior, status) in [
        (Behavior::Pause, RunStatus::Interrupted),
        (Behavior::Cancel, RunStatus::Cancelled),
        (Behavior::Fail, RunStatus::Failed),
    ] {
        let fixture = Fixture::new(Response::Text, true);
        let (policy, binding) = policy(behavior);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(binding);
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        completed(
            handle
                .stop_execution(InterruptionCause::HostShutdown, &context())
                .await
                .unwrap(),
        );
        let result = outcome(&handle).await;
        assert_eq!(result.result.status(), status);
        assert_eq!(
            result.app_state.as_ref().unwrap().status,
            id("maintenance_hold")
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn invalid_error_or_panicking_policy_uses_the_default_and_keeps_safe_diagnostics() {
    for (behavior, code) in [
        (Behavior::InvalidState, "invalid_decision"),
        (Behavior::Error, "callback_error"),
        (Behavior::Panic, "callback_panic"),
    ] {
        let fixture = Fixture::new(Response::Text, true);
        let (_, binding) = policy(behavior);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(binding);
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        completed(
            handle
                .stop_execution(InterruptionCause::HostShutdown, &context())
                .await
                .unwrap(),
        );
        let result = outcome(&handle).await;
        assert_eq!(result.result.status(), RunStatus::Interrupted);
        assert_eq!(result.app_state, None);
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        let record = fixture
            .store
            .read_record(
                &scope(),
                saved.snapshot.interruption_records.last().unwrap(),
            )
            .await
            .unwrap();
        let decision: InterruptionDecisionRecord =
            serde_json::from_value(record.value().clone()).unwrap();
        assert_eq!(decision.callback_error, Some(id(code)));
    }
}
#[tokio::test]
async fn a_policy_cannot_turn_user_cancel_into_a_pause() {
    for behavior in [Behavior::Pause, Behavior::DefaultState] {
        let fixture = Fixture::new(Response::Text, true);
        let (_, binding) = policy(behavior);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(binding);
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        completed(
            handle
                .cancel(id("user_cancelled"), &context())
                .await
                .unwrap(),
        );
        let result = outcome(&handle).await;
        assert_eq!(result.result.status(), RunStatus::Cancelled);
        assert_eq!(
            result.app_state.is_some(),
            matches!(behavior, Behavior::DefaultState)
        );
    }
}
#[tokio::test(start_paused = true)]
async fn callback_timeout_falls_back_after_one_second_without_extending_the_run_deadline() {
    let fixture = Fixture::new(Response::Text, true);
    let (policy, binding) = policy(Behavior::Pending);
    let mut bindings = fixture.bindings();
    bindings.interruption_policy = Some(binding);
    bindings.settings.lease_ttl_ms = 30_000;
    bindings.settings.heartbeat_interval_ms = 100;
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let deadline = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot
        .timing
        .deadline_at_ms;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    policy.entered.notified().await;
    tokio::time::advance(Duration::from_millis(1001)).await;
    assert_eq!(
        outcome(&handle).await.result.status(),
        RunStatus::Interrupted
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.timing.deadline_at_ms, deadline);
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.snapshot.interruption_records.last().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["callback_error"], json!("callback_timeout"));
}

async fn resume_after_release(
    agent: &Agent,
    command: ResumeCommand,
) -> Result<RunHandle, ContractError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match agent.resume(command.clone(), context()).await {
                Err(error) if error.code == ErrorCode::LeaseBusy => tokio::task::yield_now().await,
                Ok(value) => return Ok(completed(value)),
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn explicit_recovery_keeps_app_state_and_requires_the_pinned_policy() {
    let fixture = Fixture::new(Response::Text, true);
    let (_, binding) = policy(Behavior::Pause);
    let mut bindings = fixture.bindings();
    bindings.interruption_policy = Some(binding);
    let mut selected = profile();
    selected.limits.max_recovery_attempts = 1;
    let agent = create_agent(selected.clone(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    let stopped = outcome(&handle).await;
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let checkpoint = saved
        .snapshot
        .recovery_record(id("recovery-source"))
        .unwrap();
    let command = ResumeCommand {
        command_id: id("resume-stop"),
        run_id: handle.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        action: ResumeAction::Recover {
            recovery_ref: checkpoint.reference().clone(),
        },
    };
    let missing = create_agent(selected, fixture.bindings()).unwrap();
    assert_eq!(
        resume_after_release(&missing, command.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot,
        saved.snapshot
    );
    fixture.model.release.add_permits(1);
    let resumed = resume_after_release(&agent, command).await.unwrap();
    let result = outcome(&resumed).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(result.app_state, stopped.app_state);
    assert_eq!(outcome(&handle).await, stopped);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_pause_proposal_cannot_override_the_original_run_deadline() {
    let fixture = Fixture::new(Response::Text, true);
    let (_, binding) = policy(Behavior::Pause);
    let mut bindings = fixture.bindings();
    bindings.interruption_policy = Some(binding);
    bindings.settings.lease_ttl_ms = 30_000;
    bindings.settings.heartbeat_interval_ms = 100;
    let mut selected = profile();
    selected.limits.max_elapsed_ms = 100.try_into().unwrap();
    let agent = create_agent(selected, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    tokio::time::advance(Duration::from_millis(101)).await;
    assert_eq!(
        outcome(&handle).await.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let record = fixture
        .store
        .read_record(
            &scope(),
            saved.snapshot.interruption_records.last().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.value()["callback_error"], json!("invalid_decision"));
}
#[tokio::test]
async fn rejected_stop_storage_is_not_reported_as_interrupted_and_lost_ack_reuses_the_saved_result()
{
    for lost_ack in [false, true] {
        let fixture = Fixture::new(Response::Text, true);
        let store = Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            if lost_ack {
                FinalCommitMode::LoseAcknowledgement
            } else {
                FinalCommitMode::Reject
            },
        ));
        let (policy, binding) = policy(Behavior::Pause);
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        bindings.interruption_policy = Some(binding);
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        completed(
            handle
                .stop_execution(InterruptionCause::HostShutdown, &context())
                .await
                .unwrap(),
        );
        let result = tokio::time::timeout(Duration::from_secs(5), handle.outcome(&context()))
            .await
            .unwrap();
        if lost_ack {
            assert_eq!(
                completed(result.unwrap()).result.status(),
                RunStatus::Interrupted
            );
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
        }
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        assert_eq!(
            saved.snapshot.interruption_records.len(),
            usize::from(lost_ack)
        );
        assert_eq!(saved.snapshot.outcome.is_some(), lost_ack);
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.final_attempts.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn stalled_stop_persistence_is_bounded_by_the_five_second_cleanup_window() {
    let fixture = Fixture::new(Response::Text, true);
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Pause,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    bindings.settings.lease_ttl_ms = 30_000;
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    store.final_entered.notified().await;
    tokio::time::advance(Duration::from_millis(5001)).await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
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
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test(start_paused = true)]
async fn lost_ownership_never_invokes_policy_commits_a_stop_or_releases_the_old_lease() {
    let fixture = Fixture::new(Response::Text, true);
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PassThrough,
    ));
    let (policy, binding) = policy(Behavior::Pause);
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    bindings.interruption_policy = Some(binding);
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    // Advance past the lease before the renewal task can run, simulating a suspended owner.
    tokio::time::advance(Duration::from_millis(1001)).await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::LeaseLost
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.release_calls.load(Ordering::SeqCst), 0);
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.interruption_records.is_empty());
    assert!(saved.snapshot.outcome.is_none());
}

fn replace_value(value: &mut serde_json::Value, old: &serde_json::Value, new: &serde_json::Value) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        serde_json::Value::Array(values) => values
            .iter_mut()
            .for_each(|value| replace_value(value, old, new)),
        serde_json::Value::Object(values) => values
            .values_mut()
            .for_each(|value| replace_value(value, old, new)),
        _ => {}
    }
}
fn record_mut<'a>(
    image: &'a mut serde_json::Value,
    reference: &serde_json::Value,
) -> &'a mut serde_json::Value {
    &mut image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| &record["reference"] == reference)
        .unwrap()["value"]
}
fn rehash(image: &mut serde_json::Value) {
    for _ in 0..20 {
        let changes: Vec<_> = image["records"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|record| {
                let digest = json!(canonical_digest(&record["value"]));
                if record["reference"]["digest"] == digest {
                    return None;
                }
                let old = record["reference"].clone();
                let mut new = old.clone();
                new["digest"] = digest;
                Some((old, new))
            })
            .collect();
        if changes.is_empty() {
            return;
        }
        for (old, new) in changes {
            replace_value(image, &old, &new);
        }
    }
    panic!("reference cycle");
}
#[tokio::test]
async fn restored_stops_require_their_event_exact_checkpoint_and_cause_consistent_outcome() {
    for behavior in [Behavior::Pause, Behavior::Fail] {
        let fixture = Fixture::new(Response::Text, true);
        let (_, binding) = policy(behavior);
        let mut bindings = fixture.bindings();
        bindings.interruption_policy = Some(binding);
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        completed(
            handle
                .stop_execution(InterruptionCause::HostShutdown, &context())
                .await
                .unwrap(),
        );
        outcome(&handle).await;
        let image =
            serde_json::to_value(fixture.store.export_checkpoint(&scope()).unwrap()).unwrap();
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .unwrap();
        let decision_ref = image["runs"][0]["snapshot"]["interruption_records"][0].clone();
        if matches!(behavior, Behavior::Pause) {
            let mut missing = image.clone();
            missing["runs"][0]["events"]
                .as_array_mut()
                .unwrap()
                .retain(|event| event["payload"]["type"] != "run.interrupted");
            let seq = missing["runs"][0]["snapshot"]["last_event_seq"]
                .as_u64()
                .unwrap();
            missing["runs"][0]["snapshot"]["last_event_seq"] = json!(seq - 1);
            assert!(
                StateStoreCheckpoint::from_json(
                    &missing.to_string(),
                    &scope(),
                    &canonical_digest(&missing)
                )
                .is_err()
            );
            let mut stale = image.clone();
            let old = record_mut(&mut stale, &decision_ref)["interruption"].clone();
            let mut new = old.clone();
            new["checkpoint_revision"] = json!(old["checkpoint_revision"].as_u64().unwrap() - 1);
            replace_value(&mut stale, &old, &new);
            rehash(&mut stale);
            assert!(
                StateStoreCheckpoint::from_json(
                    &stale.to_string(),
                    &scope(),
                    &canonical_digest(&stale)
                )
                .is_err()
            );
        } else {
            let mut changed = image.clone();
            let decision = record_mut(&mut changed, &decision_ref);
            decision["interruption"]["cause"] = json!("user_cancel");
            decision["action"] = json!("use_default");
            rehash(&mut changed);
            assert!(
                StateStoreCheckpoint::from_json(
                    &changed.to_string(),
                    &scope(),
                    &canonical_digest(&changed)
                )
                .is_err()
            );
        }
    }
}

#[tokio::test]
async fn a_live_stop_commit_cannot_omit_the_interrupted_event() {
    let fixture = Fixture::new(Response::Text, true);
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::OmitInterruptionEvent,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store;
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    completed(
        handle
            .stop_execution(InterruptionCause::HostShutdown, &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::InvalidEvent
    );
    assert!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .interruption_records
            .is_empty()
    );
}
