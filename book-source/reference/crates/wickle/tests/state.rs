//! Atomic admission, persistence, leases, and scope isolation of the memory store.

use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};
use wickle::*;

mod support;
use support::*;

#[tokio::test]
async fn request_lookup_uses_scope_and_session_and_returns_current_state_after_restore() {
    let store = MemoryStateStore::new();
    assert!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .admit(
            &scope(),
            admission("run-a", "request", "first", "input", "1").await,
        )
        .await
        .unwrap()
        .state;
    store
        .admit(
            &scope(),
            admission("run-b", "request", "second", "input", "1").await,
        )
        .await
        .unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run-a"), &id("worker"), 0, 1000)
        .await
        .unwrap();
    let finished = store
        .commit(&scope(), &id("run-a"), finished(&first.snapshot, lease, 1))
        .await
        .unwrap();
    assert_eq!(
        store
            .find_request(&scope(), &id("first"), &id("request"))
            .await
            .unwrap(),
        Some(finished)
    );
    let checkpoint = store.export_checkpoint(&scope()).unwrap();
    let restored = MemoryStateStore::from_checkpoint(
        StateStoreCheckpoint::from_json(
            &serde_json::to_string(&checkpoint).unwrap(),
            &scope(),
            &checkpoint.digest(),
        )
        .unwrap(),
    );
    let first = restored
        .find_request(&scope(), &id("first"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    let second = restored
        .find_request(&scope(), &id("second"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.snapshot.status, RunStatus::Succeeded);
    assert_eq!(second.snapshot.run_id, id("run-b"));
    let foreign = Scope {
        workspace_id: id("foreign"),
        ..scope()
    };
    assert!(
        restored
            .find_request(&foreign, &id("first"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restored
            .find_request(&scope(), &id("missing"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn admitted_model_options_are_fixed_for_replay_and_later_commits() {
    fn with_effort(mut input: AdmissionInput, effort: &str) -> AdmissionInput {
        input.snapshot.request.model_options =
            JsonObject::from([("reasoning_effort".into(), json!(effort))]);
        input.snapshot.request_digest =
            admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
        let RunEventPayload::RunStarted { request_ref, .. } = &mut input.events[0].payload else {
            unreachable!()
        };
        let record = ProtectedRecord::new(
            request_ref.record_id.clone(),
            request_ref.revision,
            serde_json::to_value(&input.snapshot.request).unwrap(),
        );
        let old_ref = request_ref.clone();
        *request_ref = record.reference().clone();
        *input
            .records
            .iter_mut()
            .find(|record| record.reference() == &old_ref)
            .unwrap() = record;
        input
    }
    let store = MemoryStateStore::new();
    let first = with_effort(
        admission("run", "request", "session", "input", "1").await,
        "high",
    );
    let expected_options = first.snapshot.request.model_options.clone();
    let original = store.admit(&scope(), first).await.unwrap().state;
    let replay = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "high",
    );
    let replay = store.admit(&scope(), replay).await.unwrap();
    assert!(!replay.created);
    assert_eq!(
        replay.state.snapshot.request.model_options,
        expected_options
    );
    let changed = with_effort(
        admission("replacement", "request", "session", "input", "2").await,
        "low",
    );
    assert_ne!(
        changed.snapshot.request_digest,
        original.snapshot.request_digest
    );
    assert_eq!(
        store.admit(&scope(), changed).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );

    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("worker"), 1, 100)
        .await
        .unwrap();
    let mut change = prepared(&original.snapshot, lease, 2);
    change
        .snapshot
        .request
        .model_options
        .insert("reasoning_effort".into(), json!("low"));
    change.snapshot.request_digest =
        admission_digest(&change.snapshot.request, &change.snapshot.profile, None);
    assert_eq!(
        store
            .commit(&scope(), &id("run"), change)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidTransition
    );
    let saved = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let restored = RunSnapshot::from_json(&serde_json::to_string(&saved).unwrap()).unwrap();
    assert_eq!(restored.request.model_options, expected_options);
    assert_eq!(restored.request_digest, original.snapshot.request_digest);
}

#[tokio::test]
async fn identical_retries_return_the_original_run_without_replacing_resolved_metadata() {
    let store = MemoryStateStore::new();
    let first = admission("run-a", "request", "session", "input", "1").await;
    let receipt = store.admit(&scope(), first.clone()).await.unwrap();
    assert!(receipt.created);
    let retry = admission("run-b", "request", "session", "input", "2").await;
    let replay = store.admit(&scope(), retry).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run-a"));
    assert_eq!(
        replay.state.snapshot.profile.resolution_digest(),
        first.snapshot.profile.resolution_digest()
    );
    assert_eq!(replay.state.messages.len(), 1);
    let changed = admission("run-c", "request", "session", "different input", "1").await;
    assert!(store.admit(&scope(), changed).await.is_err());
    assert_eq!(
        store
            .load(&scope(), &id("run-a"))
            .await
            .unwrap()
            .snapshot
            .revision,
        0
    );
    let events = store
        .read_events(&scope(), &id("run-a"), 0, 100)
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
}

#[tokio::test]
async fn concurrent_duplicate_admission_creates_exactly_one_run() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = vec![];
    for n in 0..8 {
        let input = admission(&format!("run-{n}"), "request", "session", "input", "1").await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await.unwrap()
        }));
    }
    let mut created = 0;
    let mut ids = BTreeSet::new();
    for h in handles {
        let result = h.await.unwrap();
        created += usize::from(result.created);
        ids.insert(result.state.snapshot.run_id);
    }
    assert_eq!(created, 1);
    assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn distinct_concurrent_requests_create_only_one_active_run_in_the_session() {
    let store = Arc::new(MemoryStateStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for n in 0..8 {
        let input = admission(
            &format!("run-{n}"),
            &format!("request-{n}"),
            "session",
            "input",
            "1",
        )
        .await;
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.admit(&scope(), input).await
        }));
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(result) => accepted.push(result.state.snapshot.run_id),
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(rejected, 7);
    assert_eq!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .as_ref(),
        accepted.first()
    );
}

#[tokio::test]
async fn waiting_keeps_the_session_busy_even_after_the_worker_releases_its_lease() {
    let store = MemoryStateStore::new();
    store
        .admit(
            &scope(),
            admission("run", "request", "session", "input", "1").await,
        )
        .await
        .unwrap();
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&state.snapshot, lease.clone(), 101);
    let wait = WaitState {
        wait_id: id("wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: Some(1000),
    };
    let record = ProtectedRecord::new(id("wait-record"), 1, serde_json::to_value(&wait).unwrap());
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            outcome_ref: None,
            wait_ref: record.reference().clone(),
        },
    ));
    update.records.push(record);
    store.commit(&scope(), &id("run"), update).await.unwrap();
    store
        .release_lease(&scope(), &id("run"), &lease, 102)
        .await
        .unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.status, RunStatus::Waiting);
}

#[tokio::test]
async fn competing_requests_cannot_share_an_active_session_and_terminal_commit_releases_it() {
    let store = MemoryStateStore::new();
    let first = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), first).await.unwrap();
    assert!(
        store
            .admit(
                &scope(),
                admission("other", "other-request", "session", "other", "1").await
            )
            .await
            .is_err()
    );
    let state = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 50)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&state.snapshot, lease, 101))
        .await
        .unwrap();
    assert!(
        store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    let mut second = admission("other", "other-request", "session", "other", "1").await;
    second.messages[0].sequence = 2.try_into().unwrap();
    assert!(store.admit(&scope(), second).await.unwrap().created);
    assert_eq!(
        store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Succeeded
    );
    let replay = store
        .admit(
            &scope(),
            admission("replacement", "request", "session", "input", "2").await,
        )
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state.snapshot.run_id, id("run"));
}

#[tokio::test]
async fn lease_expiry_fencing_and_revision_conflicts_are_independent() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let first = store
        .acquire_lease(&scope(), &id("run"), &id("owner-a"), 100, 10)
        .await
        .unwrap();
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner-b"), 109, 10)
            .await
            .is_err()
    );
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 110, 10)
            .await
            .is_err()
    );
    let second = store
        .acquire_lease(&scope(), &id("run"), &id("owner-b"), 110, 10)
        .await
        .unwrap();
    assert!(second.fencing_token > first.fencing_token);
    let state = store.load(&scope(), &id("run")).await.unwrap();
    assert!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&state.snapshot, first.clone(), 111)
            )
            .await
            .is_err()
    );
    let update = prepared(&state.snapshot, second.clone(), 111);
    store
        .commit(&scope(), &id("run"), update.clone())
        .await
        .unwrap();
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    assert!(
        store
            .renew_lease(&scope(), &id("run"), &first, 111, 20)
            .await
            .is_err()
    );
    let renewed = store
        .renew_lease(&scope(), &id("run"), &second, 119, 20)
        .await
        .unwrap();
    assert_eq!(renewed.fencing_token, second.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    // Heartbeat renews expiry without invalidating the driver's same-generation copy.
    store
        .commit(
            &scope(),
            &id("run"),
            prepared(&current.snapshot, second.clone(), 125),
        )
        .await
        .unwrap();
    store
        .release_lease(&scope(), &id("run"), &renewed, 130)
        .await
        .unwrap();
    let third = store
        .acquire_lease(&scope(), &id("run"), &id("owner-c"), 130, 20)
        .await
        .unwrap();
    assert!(third.fencing_token > renewed.fencing_token);
    let current = store.load(&scope(), &id("run")).await.unwrap();
    let mut forged = third.clone();
    forged.expires_at_ms = i64::MAX;
    assert_eq!(
        store
            .commit(
                &scope(),
                &id("run"),
                prepared(&current.snapshot, forged, 150)
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseLost
    );
}

#[tokio::test]
async fn an_event_cannot_announce_a_wait_absent_from_the_committed_snapshot() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    let wrong_payload = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.last_event_seq = 2;
    update.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::RunWaiting {
            outcome_ref: None,
            wait_ref: wrong_payload,
        },
    ));
    assert_eq!(
        store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidEvent
    );
    assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), before);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn uncertain_tool_effects_keep_the_original_attempt_and_idempotency_key() {
    struct StationaryClock;
    impl Clock for StationaryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 102,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    struct AllowPolicy;
    impl PolicyPort for AllowPolicy {
        fn authorize<'a>(
            &'a self,
            _: &'a PolicyRequest,
            _: PolicyContext<'a>,
        ) -> PortFuture<'a, PolicyDecision> {
            Box::pin(async { Ok(PolicyDecision::Allow {}) })
        }
    }
    let store = Arc::new(MemoryStateStore::new());
    let registry = Arc::new(SystemInputRegistry::default());
    let compiled = SchemaCompiler::new().compile(ToolDescriptor {
        tool: VersionedRef { id: id("tool"), version: id("1") }, name: id("tool"), description: "Write a record".into(),
        input_schema: json!({"type":"object","properties":{},"required":[],"additionalProperties":false}), agent_parameters: vec![], system_bindings: None,
        output_schema: json!(true), side_effect: ToolSideEffect::Write, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: true, max_output_bytes: 1024.try_into().unwrap(),
    }, &registry).unwrap();
    let mut input = admission("run", "request", "session", "input", "1").await;
    let mut profile = input.snapshot.profile.profile().clone();
    profile.tools.push(ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("tool"),
        version: id("1"),
        bindings: None,
        config: None,
    }));
    input.snapshot.profile = ProfileValidator::new(&Catalog { revision: "1" })
        .validate(&profile, &scope())
        .await
        .unwrap();
    input.snapshot.request_digest =
        admission_digest(&input.snapshot.request, &input.snapshot.profile, None);
    if let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload {
        *profile_digest = input.snapshot.profile.profile_digest().clone();
    }
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut plan = prepared(&before.snapshot, lease.clone(), 101);
    let call = ToolCall {
        provider_arguments: None,
        call_id: id("call"),
        model_request_id: id("model-request"),
        provider_call_id: id("provider-call"),
        tool_name: id("tool"),
        model_inputs: Default::default(),
        descriptor_digest: Some(compiled.descriptor_digest().clone()),
        bound_input_ref: None,
    };
    let call_record =
        ProtectedRecord::new(id("planned-call"), 1, serde_json::to_value(&call).unwrap());
    plan.snapshot.phase = RunPhase::Tool;
    plan.snapshot.last_event_seq = 2;
    plan.snapshot.tool_ledger.push(ToolLedgerEntry {
        call,
        state: ToolCallState::Planned {},
    });
    plan.events.push(event(
        &id("run"),
        &id("session"),
        &scope(),
        2,
        RunEventPayload::ToolPlanned {
            call_ref: call_record.reference().clone(),
        },
    ));
    plan.records.push(call_record);
    store.commit(&scope(), &id("run"), plan).await.unwrap();
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope(),
            principal_ref: id("caller"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let budget = RunBudget::attach(
        store.clone(),
        Arc::new(StationaryClock),
        Arc::new(RandomIdSource),
        scope(),
        id("run"),
        lease.clone(),
        context.cancellation.clone(),
    )
    .await
    .unwrap();
    let binder = InputBinder::new(
        registry,
        None,
        Arc::new(
            PolicyGate::new(Arc::new(AllowPolicy), std::time::Duration::from_secs(1)).unwrap(),
        ),
        Arc::new(RandomIdSource),
    );
    binder
        .bind(&compiled, &id("call"), &context, &budget)
        .await
        .unwrap();
    let reservation = budget
        .reserve(ReservationKind::Tool {
            call_id: id("call"),
        })
        .await
        .unwrap();
    let planned = store.load(&scope(), &id("run")).await.unwrap();
    let mut dispatch = prepared(&planned.snapshot, lease.clone(), 102);
    dispatch.snapshot.phase = RunPhase::Tool;
    dispatch.snapshot.tool_ledger[0].state = ToolCallState::Dispatching {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let dispatched = store.commit(&scope(), &id("run"), dispatch).await.unwrap();
    let mut lost = prepared(&dispatched.snapshot, lease.clone(), 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: id("attempt-b"),
        idempotency_key: id("different-key"),
    };
    lost.snapshot.reservations.push(AttemptReservation {
        attempt_id: id("attempt-b"),
        kind: ReservationKind::Tool {
            call_id: id("call"),
        },
        reserved_at_ms: lost.snapshot.timing.last_observed_at_ms,
    });
    lost.snapshot.usage.tool_attempts += 1;
    let rejected = store.commit(&scope(), &id("run"), lost).await.unwrap_err();
    assert_eq!(rejected.code, ErrorCode::InvalidTransition, "{rejected:?}");
    let mut lost = prepared(&dispatched.snapshot, lease, 103);
    lost.snapshot.phase = RunPhase::Tool;
    lost.snapshot.tool_ledger[0].state = ToolCallState::Unknown {
        attempt_id: reservation.attempt_id.clone(),
        idempotency_key: id("effect-key"),
    };
    let saved = store.commit(&scope(), &id("run"), lost).await.unwrap();
    assert!(
        matches!(&saved.snapshot.tool_ledger[0].state, ToolCallState::Unknown { attempt_id, idempotency_key } if attempt_id == &reservation.attempt_id && idempotency_key == &id("effect-key"))
    );
}

#[tokio::test]
async fn invalid_multi_event_commit_does_not_partially_publish_records_state_or_messages() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease, 101);
    let wait = WaitState {
        wait_id: id("new-wait"),
        target: WaitTarget::Input {
            request: InputRequest {
                input_request_id: id("input-request"),
                call_id: id("input-call"),
                question: "Choose a source".into(),
                schema_ref: None,
            },
        },
        expires_at_ms: None,
    };
    let record = ProtectedRecord::new(id("new-record"), 1, serde_json::to_value(&wait).unwrap());
    let reference = record.reference().clone();
    update.records.push(record);
    update.events = vec![
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                outcome_ref: None,
                wait_ref: reference.clone(),
            },
        ),
        event(
            &id("run"),
            &id("session"),
            &scope(),
            2,
            RunEventPayload::RunWaiting {
                outcome_ref: None,
                wait_ref: reference.clone(),
            },
        ),
    ];
    update.snapshot.last_event_seq = 2;
    update.snapshot.status = RunStatus::Waiting;
    update.snapshot.phase = RunPhase::Waiting;
    update.snapshot.wait = Some(wait);
    let mut message = before.messages[0].clone();
    message.message_id = id("new-message");
    message.sequence = 2.try_into().unwrap();
    update.messages.push(message);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.session, before.session);
    assert!(store.read_record(&scope(), &reference).await.is_err());
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 0, 100)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn commits_cannot_replace_request_or_resolved_profile_and_reads_return_owned_snapshots() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let before = store.load(&scope(), &id("run")).await.unwrap();
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    let mut update = prepared(&before.snapshot, lease.clone(), 101);
    update.snapshot.request.input = vec![InputContent::Text {
        text: "replacement".into(),
    }];
    update.snapshot.request_digest =
        admission_digest(&update.snapshot.request, &update.snapshot.profile, None);
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let replacement = admission("run", "request", "session", "input", "2").await;
    let mut update = prepared(&before.snapshot, lease, 101);
    update.snapshot.profile = replacement.snapshot.profile;
    assert!(store.commit(&scope(), &id("run"), update).await.is_err());
    let mut copy = store.load(&scope(), &id("run")).await.unwrap();
    copy.messages.clear();
    copy.snapshot.request.input.clear();
    let after = store.load(&scope(), &id("run")).await.unwrap();
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(after.messages, before.messages);
}

#[tokio::test]
async fn every_store_surface_is_scoped_and_memory_does_not_claim_durability() {
    let store = MemoryStateStore::new();
    let capabilities = store.capabilities();
    assert!(
        !capabilities.durable && !capabilities.cross_process_leases && capabilities.event_replay
    );
    let mut durable = admission(
        "durable",
        "durable-request",
        "durable-session",
        "input",
        "1",
    )
    .await;
    durable.require_durable = true;
    assert!(store.admit(&scope(), durable).await.is_err());
    let input = admission("run", "request", "session", "input", "1").await;
    let record = input.records[0].reference().clone();
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    for foreign in [
        Scope {
            tenant_id: id("other"),
            ..scope()
        },
        Scope {
            workspace_id: id("other"),
            ..scope()
        },
        Scope {
            user_id: Some(id("other")),
            ..scope()
        },
    ] {
        assert!(store.load(&foreign, &id("run")).await.is_err());
        assert!(store.load_session(&foreign, &id("session")).await.is_err());
        assert!(
            store
                .read_events(&foreign, &id("run"), 0, 100)
                .await
                .is_err()
        );
        assert!(store.read_record(&foreign, &record).await.is_err());
        assert!(
            store
                .acquire_lease(&foreign, &id("run"), &id("owner"), 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .renew_lease(&foreign, &id("run"), &lease, 101, 10)
                .await
                .is_err()
        );
        assert!(
            store
                .commit(
                    &foreign,
                    &id("run"),
                    prepared(&snapshot, lease.clone(), 101)
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn event_pages_are_exclusive_ordered_replayable_and_preserved_after_completion() {
    let store = MemoryStateStore::new();
    let input = admission("run", "request", "session", "input", "1").await;
    store.admit(&scope(), input).await.unwrap();
    let snapshot = store.load(&scope(), &id("run")).await.unwrap().snapshot;
    let lease = store
        .acquire_lease(&scope(), &id("run"), &id("owner"), 100, 100)
        .await
        .unwrap();
    store
        .commit(&scope(), &id("run"), finished(&snapshot, lease, 101))
        .await
        .unwrap();
    let first = store.read_events(&scope(), &id("run"), 0, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.has_more);
    assert_eq!(first.next_after_seq, 1);
    let second = store
        .read_events(&scope(), &id("run"), first.next_after_seq, 1)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].seq.get(), 2);
    assert!(!second.has_more);
    assert_eq!(
        store
            .read_events(&scope(), &id("run"), 1, 100)
            .await
            .unwrap()
            .events,
        second.events
    );
    assert!(
        store
            .read_events(&scope(), &id("run"), 2, 100)
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(
        store
            .acquire_lease(&scope(), &id("run"), &id("owner"), 102, 100)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn recovery_acceptance_requires_the_exact_running_checkpoint_and_its_event() {
    for mode in ["valid", "missing-event", "changed-source"] {
        let store = MemoryStateStore::new();
        let saved = store
            .admit(
                &scope(),
                admission("run", "request", "session", "input", "1").await,
            )
            .await
            .unwrap()
            .state;
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("recovery-worker"), 0, 1000)
            .await
            .unwrap();
        let mut source = saved.snapshot.clone();
        if mode == "changed-source" {
            source.phase = RunPhase::Tool;
        }
        let source = source.recovery_record(id("source-checkpoint")).unwrap();
        let command = ResumeCommand {
            run_id: id("run"),
            expected_revision: saved.snapshot.revision,
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
            command,
            command_ref: command_record.reference().clone(),
            source_snapshot_ref: source.reference().clone(),
            accepted_revision: saved.snapshot.revision + 1,
            previous_segment_start_revision: 0,
            previous_last_event_seq: saved.snapshot.last_event_seq,
            actor_ref: id("operator"),
            capability_grant_ref: id("grant"),
            expired: false,
            recovery_attempt_id: Some(id("recovery-budget")),
        };
        let record = ProtectedRecord::new(
            id("recovery-receipt"),
            1,
            serde_json::to_value(&receipt).unwrap(),
        );
        let mut update = prepared(&saved.snapshot, lease, 0);
        update.snapshot.recovery_receipts.push(receipt);
        update.snapshot.usage.recovery_attempts += 1;
        update.snapshot.reservations.push(AttemptReservation {
            attempt_id: id("recovery-budget"),
            kind: ReservationKind::Recovery {},
            reserved_at_ms: 0,
        });
        if mode != "missing-event" {
            update.snapshot.last_event_seq += 1;
            update.events.push(event(
                &id("run"),
                &id("session"),
                &scope(),
                update.snapshot.last_event_seq,
                RunEventPayload::RunRecovered {
                    recovery_receipt_ref: record.reference().clone(),
                },
            ));
        }
        for event in &mut update.events {
            event.timestamp_ms = 0;
        }
        update.records = vec![source, command_record, record];
        let result = store.commit(&scope(), &id("run"), update).await;
        if mode == "valid" {
            let result = result.unwrap();
            assert_eq!(result.snapshot.status, RunStatus::Running);
            assert!(result.snapshot.outcome.is_none());
            let checkpoint = store.export_checkpoint(&scope()).unwrap();
            let restored = StateStoreCheckpoint::from_json(
                &serde_json::to_string(&checkpoint).unwrap(),
                &scope(),
                &checkpoint.digest(),
            )
            .unwrap();
            assert_eq!(
                MemoryStateStore::from_checkpoint(restored)
                    .load(&scope(), &id("run"))
                    .await
                    .unwrap(),
                result
            );
        } else {
            assert!(result.is_err(), "{mode}");
            assert_eq!(store.load(&scope(), &id("run")).await.unwrap(), saved);
        }
    }
}

#[tokio::test]
async fn admission_race_compares_stored_submission_before_candidate_configuration() {
    let store = MemoryStateStore::new();
    assert!(
        store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    let mut first = admission("winner", "request", "session", "same", "1").await;
    let profile = first.snapshot.profile.profile();
    first.submitted = Some(
        RequestSnapshot::capture(
            VersionedRef {
                id: profile.agent_id.clone(),
                version: profile.version.clone(),
            },
            &serde_json::to_string(&first.snapshot.request).unwrap(),
            None,
            JsonTextLimits::default(),
        )
        .unwrap(),
    );
    let original = first.submitted.clone();
    let winner = store.admit(&scope(), first).await.unwrap();
    let mut loser = admission("loser", "request", "session", "same", "99").await;
    loser.submitted = original;
    loser.snapshot.request_digest = canonical_digest(&json!("changed-current-configuration"));
    let replay = store.admit(&scope(), loser.clone()).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.state, winner.state);
    let changed = RequestSnapshot::capture(
        VersionedRef {
            id: id("assistant"),
            version: id("1.0.0"),
        },
        &serde_json::to_string(
            &admission("unused", "request", "session", "different", "1")
                .await
                .snapshot
                .request,
        )
        .unwrap(),
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    loser.submitted = Some(changed);
    assert_eq!(
        store.admit(&scope(), loser).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
}
