//! Recovery commands retain the original checkpoint and do not steal live execution.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use std::sync::{Arc, atomic::Ordering};
use support::*;
use wickle::*;

fn recovery_profile() -> AgentProfile {
    let mut profile = profile();
    profile.limits.max_recovery_attempts = 2;
    profile
}
fn recovery_agent(fixture: &Fixture) -> Agent {
    create_agent(recovery_profile(), fixture.bindings()).unwrap()
}
fn recovery(snapshot: &RunSnapshot, name: &str) -> ResumeCommand {
    let record = snapshot
        .recovery_record(id(&format!("recovery-source-{name}")))
        .unwrap();
    ResumeCommand {
        run_id: snapshot.run_id.clone(),
        expected_revision: snapshot.revision,
        command_id: id(name),
        action: ResumeAction::Recover {
            recovery_ref: record.reference().clone(),
        },
    }
}
async fn unfinished(fixture: &Fixture) -> (RunHandle, RunSnapshot) {
    let mut bindings = fixture.bindings();
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Reject,
    ));
    let agent = create_agent(recovery_profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let snapshot = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, RunStatus::Running);
    assert!(snapshot.candidate_ref.is_some());
    (handle, snapshot)
}
#[tokio::test]
async fn recovering_a_saved_candidate_finishes_without_another_model_call() {
    let fixture = Fixture::new(Response::Text, false);
    let (old, snapshot) = unfinished(&fixture).await;
    let command = recovery(&snapshot, "recover-once");
    let agent = recovery_agent(&fixture);
    let handle = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(handle.run_id(), old.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.recovery_receipts.len(), 1);
    assert_eq!(saved.snapshot.usage.recovery_attempts, 1);
    assert!(saved.snapshot.resume_receipts.is_empty());
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        outcome
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn recovery_does_not_steal_an_active_model_lease() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = recovery_agent(&fixture);
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let snapshot = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    let result = recovery_agent(&fixture)
        .resume(recovery(&snapshot, "recover-active"), context())
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::LeaseBusy);
    assert!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .recovery_receipts
            .is_empty()
    );
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn recovery_rejects_a_changed_source_checkpoint_without_consuming_the_command() {
    let fixture = Fixture::new(Response::Text, false);
    let (handle, mut snapshot) = unfinished(&fixture).await;
    let original = snapshot.clone();
    snapshot.phase = RunPhase::Prepare;
    let result = recovery_agent(&fixture)
        .resume(recovery(&snapshot, "changed-source"), context())
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::RequestConflict);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot,
        original
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_reuses_a_completed_model_step_before_candidate_storage() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::RejectCandidate,
    ));
    let agent = create_agent(recovery_profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.phase, RunPhase::Model);
    assert!(saved.snapshot.candidate_ref.is_none());
    assert!(matches!(
        saved.snapshot.model_ledger[0].state,
        ModelAttemptState::Completed {}
    ));
    let recovered = completed(
        recovery_agent(&fixture)
            .resume(recovery(&saved.snapshot, "recover-model"), context())
            .await
            .unwrap(),
    );
    let outcome = completed(recovered.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .model_step_id,
        saved.snapshot.model_step_id
    );
}

#[tokio::test]
async fn recovery_budget_exhaustion_leaves_the_checkpoint_and_command_unaccepted() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Reject,
    ));
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let before = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let error = fixture
        .agent()
        .resume(recovery(&before.snapshot, "no-budget"), context())
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::BudgetExceeded);
    let after = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.session, before.session);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn expired_recovery_finalizes_without_dispatch_or_a_recovery_reservation() {
    let fixture = Fixture::new(Response::Text, false);
    let (old, snapshot) = unfinished(&fixture).await;
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    let agent = recovery_agent(&fixture);
    let handle = completed(
        agent
            .resume(recovery(&snapshot, "expired-recovery"), context())
            .await
            .unwrap(),
    );
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert!(matches!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    ));
    let saved = fixture.store.load(&scope(), old.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.usage.recovery_attempts, 0);
    assert!(saved.snapshot.recovery_receipts[0].expired);
    assert!(
        saved.snapshot.recovery_receipts[0]
            .recovery_attempt_id
            .is_none()
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_retries_an_interrupted_model_without_refunding_or_reusing_its_attempt() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::RejectModelResult,
    ));
    let agent = create_agent(recovery_profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let before = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(matches!(
        before.snapshot.model_ledger[0].state,
        ModelAttemptState::Reserved {}
    ));
    let agent = recovery_agent(&fixture);
    let recovered = completed(
        agent
            .resume(
                recovery(&before.snapshot, "recover-incomplete-model"),
                context(),
            )
            .await
            .unwrap(),
    );
    let outcome = completed(recovered.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    let after = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(after.snapshot.usage.model_calls, 2);
    assert_eq!(after.snapshot.usage.recovery_attempts, 1);
    let attempts = &after.snapshot.model_ledger;
    assert_eq!(attempts.len(), 2);
    assert!(matches!(
        attempts[0].state,
        ModelAttemptState::Interrupted { .. }
    ));
    assert!(attempts[0].response_ref.is_none());
    assert!(matches!(attempts[1].state, ModelAttemptState::Completed {}));
    assert_eq!(attempts[0].model_step_id, attempts[1].model_step_id);
    assert_eq!(attempts[0].route, attempts[1].route);
    assert_ne!(attempts[0].attempt_id, attempts[1].attempt_id);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let checkpoint = StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let restored = MemoryStateStore::from_checkpoint(checkpoint);
    assert_eq!(
        restored
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot,
        after.snapshot
    );
}

#[tokio::test(start_paused = true)]
async fn lost_recovery_acknowledgement_does_not_guess_ownership_or_charge_a_duplicate_command() {
    let fixture = Fixture::new(Response::Text, false);
    let (old, snapshot) = unfinished(&fixture).await;
    let mut bindings = fixture.bindings();
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::LoseRecoveryAcknowledgement,
    ));
    let agent = create_agent(recovery_profile(), bindings).unwrap();
    let command = recovery(&snapshot, "lost-recovery-ack");
    assert_eq!(
        agent
            .resume(command.clone(), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::PersistenceUnavailable
    );
    let saved = fixture.store.load(&scope(), old.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.recovery_receipts.len(), 1);
    assert_eq!(saved.snapshot.usage.recovery_attempts, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(replay.run_id(), old.run_id());
    assert_eq!(
        fixture
            .store
            .load(&scope(), old.run_id())
            .await
            .unwrap()
            .snapshot,
        saved.snapshot
    );
    // Only a new recovery after expiry can obtain ownership and launch a driver.
    tokio::time::advance(std::time::Duration::from_millis(1001)).await;
    let recovered = completed(
        recovery_agent(&fixture)
            .resume(recovery(&saved.snapshot, "confirmed-owner"), context())
            .await
            .unwrap(),
    );
    assert_eq!(
        completed(recovered.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_checks_current_permission_and_cancellation_before_acceptance() {
    for deny in [true, false] {
        let fixture = Fixture::new(Response::Text, false);
        let (handle, snapshot) = unfinished(&fixture).await;
        let context = context();
        if deny {
            fixture.policy.deny.store(4, Ordering::SeqCst);
        } else {
            context.cancellation.cancel();
        }
        let error = recovery_agent(&fixture)
            .resume(recovery(&snapshot, "blocked-recovery"), context)
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            if deny {
                ErrorCode::AccessDenied
            } else {
                ErrorCode::Cancelled
            }
        );
        assert_eq!(
            fixture
                .store
                .load(&scope(), handle.run_id())
                .await
                .unwrap()
                .snapshot,
            snapshot
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn recovery_read_lease_and_acceptance_outages_return_only_currently_authorized_checkpoint_diagnostics()
 {
    for mode in [
        FinalCommitMode::PassThrough,
        FinalCommitMode::RejectRecoveryLease,
        FinalCommitMode::RejectRecoveryAcceptance,
    ] {
        for deny_details in [false, true] {
            let fixture = Fixture::new(Response::Text, false);
            let (handle, snapshot) = unfinished(&fixture).await;
            let mut bindings = fixture.bindings();
            let store = Arc::new(FinalCommitStore::new(fixture.store.clone(), mode));
            bindings.state = store.clone();
            let agent = create_agent(recovery_profile(), bindings).unwrap();
            completed(
                agent
                    .get_run_details(handle.run_id(), &context())
                    .await
                    .unwrap(),
            );
            if matches!(mode, FinalCommitMode::PassThrough) {
                store.block_read.store(4, Ordering::SeqCst);
            }
            if deny_details {
                fixture.policy.deny.store(1, Ordering::SeqCst);
            }
            let error = agent
                .resume(recovery(&snapshot, "storage-failure"), context())
                .await
                .unwrap_err();
            if deny_details {
                assert_eq!(error.code, ErrorCode::AccessDenied);
                assert!(error.persistence.is_none());
            } else {
                assert_eq!(error.code, ErrorCode::PersistenceUnavailable);
                let diagnostic = error
                    .persistence
                    .expect("known checkpoint must survive recovery storage error");
                assert_eq!(diagnostic.run_id, *handle.run_id());
                assert_eq!(diagnostic.last_confirmed_revision, snapshot.revision);
                assert!(diagnostic.unconfirmed_effects.is_empty());
            }
            assert_eq!(
                fixture
                    .store
                    .load(&scope(), handle.run_id())
                    .await
                    .unwrap()
                    .snapshot,
                snapshot
            );
            assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
        }
    }
}
