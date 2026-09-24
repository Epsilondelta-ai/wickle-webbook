use super::*;
use std::sync::Arc;

pub async fn atomic_execution_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("atomic-run");
    let initial = store
        .admit(
            &s,
            admission(run.as_str(), "request", "session", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    let history = store.read_execution(&s, &run).await.unwrap();
    assert!(!history.initial_claimed);
    let first_id = history.segments[0].segment_id.clone();
    let claim = |segment_id| BeginSegmentRequest {
        transition: None,
        run_id: run.clone(),
        expected_revision: 0,
        segment_id,
        owner: id("worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Initial,
    };
    // A failure after tentative lease acquisition must not publish the lease.
    assert!(
        store
            .begin_segment(&s, claim(id("wrong-initial-id")))
            .await
            .is_err()
    );
    assert!(
        !store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .initial_claimed
    );
    let lease = store
        .acquire_lease(&s, &run, &id("rollback-probe"), 0, 1000)
        .await
        .unwrap();
    store.release_lease(&s, &run, &lease, 0).await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut claims = Vec::new();
    for request in [claim(first_id.clone()), claim(first_id)] {
        let store = store.clone();
        let owner = s.clone();
        let gate = gate.clone();
        claims.push(tokio::spawn(async move {
            gate.wait().await;
            store.begin_segment(&owner, request).await
        }));
    }
    let b = claims.pop().unwrap().await.unwrap();
    let a = claims.pop().unwrap().await.unwrap();
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(
        usize::from(a.lease.is_some()) + usize::from(b.lease.is_some()),
        1
    );
    let lease = a.lease.or(b.lease).unwrap();
    store.release_lease(&s, &run, &lease, 1).await.unwrap();
    let command = ControlCommand {
        command_id: id("cancel"),
        principal_ref: id("reviewer"),
        action: ControlAction::Cancel { reason: id("stop") },
    };
    let (a, b) = tokio::join!(
        store.submit_control_command(&s, &run, command.clone()),
        store.submit_control_command(&s, &run, command.clone())
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        store.read_execution(&s, &run).await.unwrap().controls.len(),
        1
    );
    let mut changed = command;
    changed.principal_ref = id("different-user");
    assert_eq!(
        store
            .submit_control_command(&s, &run, changed)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RequestConflict
    );
    let mut commit = finished(&initial.snapshot, lease, 2);
    commit.snapshot.status = RunStatus::Cancelled;
    commit.snapshot.outcome.as_mut().unwrap().result = OutcomeResult::Cancelled {
        reason: "stop".into(),
    };
    let outcome = ProtectedRecord::new(
        id("cancel-outcome"),
        1,
        serde_json::to_value(commit.snapshot.outcome.as_ref().unwrap()).unwrap(),
    );
    commit.events[0].payload = RunEventPayload::RunFinished {
        outcome_ref: outcome.reference().clone(),
    };
    let transition = SegmentTransition {
        snapshot: commit.snapshot,
        messages: commit.messages,
        events: commit.events,
        records: vec![outcome],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("cancel-segment"),
        owner: id("controller"),
        now_ms: 2,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Control(id("cancel")),
    };
    let mut reused = request.clone();
    reused.segment_id = history.segments[0].segment_id.clone();
    assert!(
        store.begin_segment(&s, reused).await.is_err(),
        "a control must allocate a new segment ID"
    );
    let (a, b) = tokio::join!(
        store.begin_segment(&s, request.clone()),
        store.begin_segment(&s, request)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.segment, b.segment);
    assert!(a.lease.is_none() && b.lease.is_none());
    assert_eq!(a.state.snapshot.status, RunStatus::Cancelled);
    let history = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(history.segments.len(), 2);
    assert_eq!(history.accepted_commands.len(), 1);
    assert_eq!(
        history.controls[0].processed_segment_id,
        Some(id("cancel-segment"))
    );
    assert_eq!(history.execution_principal_ref, id("execution-principal"));
    assert!(matches!(
        history.segments[0].outcome,
        Some(SegmentOutcome::Interrupted { .. })
    ));
    assert!(matches!(
        history.segments[1].outcome,
        Some(SegmentOutcome::Settled { .. })
    ));
    assert!(
        store
            .load(&s, &run)
            .await
            .unwrap()
            .session
            .active_run_id
            .is_none()
    );
    let before = store.read_events(&s, &run, 0, 100).await.unwrap();
    store
        .submit_control_command(
            &s,
            &run,
            ControlCommand {
                command_id: id("terminal-noop"),
                principal_ref: id("reviewer"),
                action: ControlAction::Expire,
            },
        )
        .await
        .unwrap();
    let after = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(after.segments, history.segments);
    assert_eq!(after.controls.len(), history.controls.len() + 1);
    assert_eq!(
        after.controls.last().unwrap().processed_segment_id,
        Some(id("cancel-segment"))
    );
    assert_eq!(store.read_events(&s, &run, 0, 100).await.unwrap(), before);
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..s
    };
    assert!(store.read_execution(&foreign, &run).await.is_err());
}

pub async fn atomic_recovery_contract(store: Arc<dyn StateStore>) {
    let s = scope();
    let run = id("recover-run");
    let saved = store
        .admit(
            &s,
            admission(
                run.as_str(),
                "recover-request",
                "recover-session",
                "input",
                "1",
            )
            .await,
        )
        .await
        .unwrap()
        .state;
    let source = saved.snapshot.recovery_record(id("source")).unwrap();
    let command = ResumeCommand {
        run_id: run.clone(),
        expected_revision: 0,
        command_id: id("recover-once"),
        action: ResumeAction::Recover {
            recovery_ref: source.reference().clone(),
        },
    };
    let command_record = ProtectedRecord::new(
        id("recovery-command"),
        1,
        serde_json::to_value(&command).unwrap(),
    );
    let receipt = RecoveryReceipt {
        command: command.clone(),
        command_ref: command_record.reference().clone(),
        source_snapshot_ref: source.reference().clone(),
        accepted_revision: 1,
        previous_segment_start_revision: 0,
        previous_last_event_seq: 1,
        actor_ref: id("reviewer"),
        capability_grant_ref: id("grant"),
        expired: false,
        recovery_attempt_id: Some(id("recovery-budget")),
    };
    let receipt_record = ProtectedRecord::new(
        id("recovery-receipt"),
        1,
        serde_json::to_value(&receipt).unwrap(),
    );
    let mut next = saved.snapshot.clone();
    next.revision = 1;
    next.phase = RunPhase::Prepare;
    next.recovery_receipts.push(receipt);
    next.usage.recovery_attempts += 1;
    next.reservations.push(AttemptReservation {
        attempt_id: id("recovery-budget"),
        kind: ReservationKind::Recovery {},
        reserved_at_ms: 0,
    });
    next.last_event_seq = 2;
    let mut e = event(
        &run,
        &id("recover-session"),
        &s,
        2,
        RunEventPayload::RunRecovered {
            recovery_receipt_ref: receipt_record.reference().clone(),
        },
    );
    e.timestamp_ms = 0;
    let transition = SegmentTransition {
        snapshot: next,
        messages: vec![],
        events: vec![e],
        records: vec![source, command_record, receipt_record],
    };
    let request = BeginSegmentRequest {
        transition: Some(transition),
        run_id: run.clone(),
        expected_revision: 0,
        segment_id: id("recovered"),
        owner: id("recover-worker"),
        now_ms: 0,
        lease_ttl_ms: 1000.try_into().unwrap(),
        start: SegmentStart::Resume(command),
    };
    let mut broken = request.clone();
    broken.transition.as_mut().unwrap().events.clear();
    assert!(store.begin_segment(&s, broken).await.is_err());
    assert_eq!(store.load(&s, &run).await.unwrap(), saved);
    assert!(
        store
            .read_execution(&s, &run)
            .await
            .unwrap()
            .accepted_commands
            .is_empty()
    );
    let before = store.read_execution(&s, &run).await.unwrap();
    let accepted = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(accepted.lease.is_some());
    let replay = store.begin_segment(&s, request.clone()).await.unwrap();
    assert!(replay.lease.is_none());
    assert_eq!(accepted.segment, replay.segment);
    assert_eq!(replay.state.snapshot.usage.recovery_attempts, 1);
    let mut conflict = request;
    conflict.expected_revision = 1;
    if let SegmentStart::Resume(c) = &mut conflict.start {
        c.expected_revision = 1;
    }
    assert!(matches!(
        store.begin_segment(&s, conflict).await,
        Err(ContractError {
            code: ErrorCode::RequestConflict,
            ..
        })
    ));
    let after = store.read_execution(&s, &run).await.unwrap();
    assert_eq!(after.segments.len(), before.segments.len() + 1);
    assert_eq!(after.accepted_commands.len(), 1);
    assert_eq!(after.execution_principal_ref, id("execution-principal"));
}

pub async fn conflicting_submissions_race(stores: [Arc<dyn StateStore>; 2]) {
    let store = stores[0].clone();
    let mut left = admission("race-left", "same-key", "race-session", "left input", "1").await;
    let mut right = admission("race-right", "same-key", "race-session", "right input", "1").await;
    for input in [&mut left, &mut right] {
        let profile = input.snapshot.profile.profile();
        input.submitted = Some(
            RequestSnapshot::capture(
                VersionedRef {
                    id: profile.agent_id.clone(),
                    version: profile.version.clone(),
                },
                &serde_json::to_string(&input.snapshot.request).unwrap(),
                None,
                Default::default(),
            )
            .unwrap(),
        );
    }
    let submitted = [
        left.submitted.clone().unwrap(),
        right.submitted.clone().unwrap(),
    ];
    let expected = [left.snapshot.clone(), right.snapshot.clone()];
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for (store, input) in stores.into_iter().zip([left, right]) {
        let gate = gate.clone();
        workers.push(tokio::spawn(async move {
            gate.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let a = workers.pop().unwrap().await.unwrap();
    let b = workers.pop().unwrap().await.unwrap();
    let (winner, loser) = match (a, b) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        _ => panic!("conflicting submissions must have exactly one admitted winner"),
    };
    assert!(winner.created);
    assert_eq!(loser.code, ErrorCode::RequestConflict);
    let loaded = store
        .find_request(&scope(), &id("race-session"), &id("same-key"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded, winner.state);
    assert!(expected.iter().any(|snapshot| snapshot == &loaded.snapshot));
    let history = store
        .read_execution(&scope(), &loaded.snapshot.run_id)
        .await
        .unwrap();
    let winner_index = expected
        .iter()
        .position(|snapshot| snapshot.run_id == loaded.snapshot.run_id)
        .unwrap();
    assert_eq!(history.submitted.as_ref(), Some(&submitted[winner_index]));
    let losing_id = expected
        .iter()
        .find(|snapshot| snapshot.run_id != loaded.snapshot.run_id)
        .unwrap()
        .run_id
        .clone();
    assert_eq!(
        store.load(&scope(), &losing_id).await.unwrap_err().code,
        ErrorCode::StateNotFound
    );
    assert_eq!(
        store
            .load_session(&scope(), &id("race-session"))
            .await
            .unwrap()
            .active_run_id,
        Some(loaded.snapshot.run_id)
    );
}
