//! Agent lifecycle, saved outcomes, request identity, and detached execution.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::StreamExt;
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[test]
fn starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks() {
    use std::{future::Future, task::Context};
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let future = agent.start(request("request"), context());
    let mut future = std::pin::pin!(future);
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let result = future.as_mut().poll(&mut context);
    assert!(matches!(
        result,
        std::task::Poll::Ready(Err(ContractError {
            code: ErrorCode::RuntimeUnavailable,
            ..
        }))
    ));
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    {
        let start = agent.start(request("request"), context());
        tokio::pin!(start);
        assert!(futures_util::poll!(start.as_mut()).is_pending());
    }
    fixture.model.entered.notified().await;
    let saved = fixture
        .store
        .find_request(&scope(), &id("session"), &id("request"))
        .await
        .unwrap()
        .unwrap();
    fixture.model.release.add_permits(1);
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(handle.run_id(), &saved.snapshot.run_id);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn construction_does_not_resolve_metadata_authorize_generate_estimate_or_allocate_ids() {
    let fixture = Fixture::new(Response::Text, false);
    let _agent = fixture.agent();
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.policy.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.queries.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.router.snapshots.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.inspector.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.estimator.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ids.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_text_turn_finishes_with_the_stored_outcome_as_authority() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.outcome, Some(outcome.clone()));
    assert_eq!(saved.snapshot.revision, outcome.checkpoint_revision);
    assert_eq!(saved.session.active_run_id, None);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let events = fixture
        .store
        .read_events(&scope(), handle.run_id(), 0, 100)
        .await
        .unwrap();
    assert!(matches!(
        events.events.last().unwrap().payload,
        RunEventPayload::RunFinished { .. }
    ));
    let view = completed(agent.get_run(handle.run_id(), &context()).await.unwrap());
    assert_eq!(view.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn dropping_the_handle_outcome_waiter_and_event_stream_does_not_cancel_the_driver() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let run_id = handle.run_id().clone();
    {
        let context = context();
        let outcome = handle.outcome(&context);
        tokio::pin!(outcome);
        assert!(futures_util::poll!(outcome.as_mut()).is_pending());
    }
    {
        let mut events = handle.events(0, context());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.run_id, run_id);
    }
    drop(handle);
    fixture.model.release.add_permits(1);
    let replay = completed(agent.start(request("request"), context()).await.unwrap());
    let outcome = completed(replay.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_requests_reuse_the_run_before_new_metadata_resolution_and_changed_options_conflict()
 {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "request").await;
    completed(first.outcome(&context()).await.unwrap());
    let resolutions = fixture.catalog.calls.load(Ordering::SeqCst);
    fixture.catalog.revision.store(99, Ordering::SeqCst);
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), first.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), resolutions);
    let mut changed = request("request");
    changed
        .model_options
        .insert("effort".into(), serde_json::json!("high"));
    assert_eq!(
        agent.start(changed, context()).await.unwrap_err().code,
        ErrorCode::RequestConflict
    );
}

#[tokio::test]
async fn a_second_request_is_busy_until_the_active_run_finishes() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    fixture.model.entered.notified().await;
    assert_eq!(
        agent
            .start(request("second"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionBusy
    );
    fixture.model.release.add_permits(1);
    completed(first.outcome(&context()).await.unwrap());
    let original = fixture
        .store
        .load(&scope(), first.run_id())
        .await
        .unwrap()
        .session
        .prompt_snapshot;
    let second = fixture.started(&agent, "second").await;
    fixture.model.release.add_permits(1);
    completed(second.outcome(&context()).await.unwrap());
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(
        fixture
            .store
            .load(&scope(), second.run_id())
            .await
            .unwrap()
            .session
            .prompt_snapshot,
        original
    );
    let requests = fixture.model.requests.lock().unwrap();
    let systems = |request: &ModelRequest| {
        request
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::System)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(systems(&requests[0]), systems(&requests[1]));
}

#[tokio::test]
async fn foreign_scope_and_current_read_or_cancel_denials_do_not_control_an_existing_run() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let mut foreign = context();
    foreign.data.scope.tenant_id = id("foreign");
    assert!(agent.get_run(handle.run_id(), &foreign).await.is_err());
    assert!(handle.cancel(id("cancel"), &foreign).await.is_err());
    fixture.policy.deny.store(1, Ordering::SeqCst);
    assert!(agent.get_run(handle.run_id(), &context()).await.is_err());
    assert!(handle.outcome(&context()).await.is_err());
    fixture.policy.deny.store(2, Ordering::SeqCst);
    assert!(handle.cancel(id("cancel"), &context()).await.is_err());
    fixture.policy.deny.store(0, Ordering::SeqCst);
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn cancelling_a_running_request_preserves_its_reserved_attempt_and_saves_cancellation() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let receipt = completed(
        handle
            .cancel(id("user_cancelled"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(&receipt.run_id, handle.run_id());
    assert!(receipt.processed_segment_id.is_none());
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    assert_eq!(outcome.usage.model_calls, 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.status, RunStatus::Cancelled);
    assert_eq!(saved.reservations.len(), 1);
    let repeated = completed(handle.cancel(id("again"), &context()).await.unwrap());
    assert!(repeated.processed_segment_id.is_some());
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap()),
        outcome
    );
}

#[tokio::test]
async fn classified_model_failure_is_saved_with_its_partial_output_instead_of_success() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Failed);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
}

#[tokio::test]
async fn unsupported_profile_modes_are_rejected_before_resolution_or_model_calls() {
    let fixture = Fixture::new(Response::Text, false);
    let mut verified = profile();
    verified.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("verifier"),
    };
    assert!(
        create_agent(verified, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .is_err()
    );
    let mut tools = profile();
    tools.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
        tool_id: id("search"),
        version: id("1"),
        bindings: None,
        config: None,
    })];
    assert!(
        create_agent(tools, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .is_err()
    );
    let mut hooks = profile();
    hooks.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("hook"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    assert_eq!(
        create_agent(hooks, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    let mut sources = profile();
    sources.context_sources = Some(vec![ContextSourceBinding {
        source: ContextSourceRef::Catalog(CatalogSourceRef {
            source_id: id("source"),
            version: id("1"),
        }),
        trigger: ContextTrigger::RunStart,
        required: true,
        timeout_ms: 1000.try_into().unwrap(),
        max_items: 1.try_into().unwrap(),
        max_bytes: 1024.try_into().unwrap(),
        max_tokens: 128.try_into().unwrap(),
    }]);
    assert_eq!(
        create_agent(sources, fixture.bindings())
            .unwrap()
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_changed_profile_cannot_replace_a_completed_sessions_pinned_prompt() {
    let fixture = Fixture::new(Response::Text, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut changed = profile();
    changed.version = id("2.0.0");
    let other = create_agent(changed, fixture.bindings()).unwrap();
    assert!(other.start(request("second"), context()).await.is_err());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("second"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancellation_receipt_is_not_a_terminal_outcome_until_the_final_commit_succeeds() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Pause,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let receipt = completed(handle.cancel(id("cancel"), &context()).await.unwrap());
    assert!(receipt.processed_segment_id.is_none());
    store.final_entered.notified().await;
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.status, RunStatus::Running);
    assert!(saved.snapshot.outcome.is_none());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    store.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Cancelled
    );
}

#[tokio::test]
async fn failed_final_storage_never_reports_a_successful_outcome_or_finished_event() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::Reject,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.outcome.is_none());
    assert!(!saved.snapshot.status.is_terminal());
    assert!(
        !fixture
            .store
            .read_events(&scope(), handle.run_id(), 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(|event| matches!(event.payload, RunEventPayload::RunFinished { .. }))
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_lost_final_commit_ack_is_resolved_from_stored_success_without_reexecuting_the_model() {
    let fixture = Fixture::new(Response::Text, false);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::LoseAcknowledgement,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .outcome,
        Some(outcome)
    );
    let duplicate = fixture.started(&agent, "request").await;
    assert_eq!(duplicate.run_id(), handle.run_id());
    completed(duplicate.outcome(&context()).await.unwrap());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.final_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_keeps_a_long_running_model_attempt_owned_beyond_the_original_lease() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    for _ in 0..15 {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    let now = fixture.clock.now().unwrap().utc_ms;
    assert_eq!(
        fixture
            .store
            .acquire_lease(&scope(), handle.run_id(), &id("competitor"), now, 1000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::LeaseBusy
    );
    fixture.model.release.add_permits(1);
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_exhaustion_stops_an_incomplete_stream_and_preserves_the_attempt() {
    let fixture = Fixture::new(Response::WaitAfterText, false);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Exhausted
    );
}

#[tokio::test]
async fn impossible_token_estimates_fail_before_model_dispatch() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.estimator.tokens.store(8192, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_ne!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.usage.model_calls, 0);
}

#[tokio::test]
async fn buffered_events_recheck_current_permission_and_observer_cancellation_before_delivery() {
    for cancel in [false, true] {
        let fixture = Fixture::new(Response::Text, true);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        let mut events = handle.events(0, observer.clone());
        let first = events.next().await.unwrap().unwrap();
        assert_eq!(first.event_type, "run.started");
        if cancel {
            observer.cancellation.cancel();
        } else {
            fixture.policy.deny.store(1, Ordering::SeqCst);
        }
        let second = events
            .next()
            .await
            .expect("observer receives a denial, not an event");
        assert_eq!(
            second.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::AccessDenied
            }
        );
        fixture.policy.deny.store(0, Ordering::SeqCst);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
    }
}

#[tokio::test]
async fn an_event_committed_between_empty_page_and_terminal_read_is_still_delivered() {
    let fixture = Fixture::new(Response::Text, true);
    let store = std::sync::Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PauseEmptyEventPage,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    fixture.model.entered.notified().await;
    let before = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot
        .last_event_seq;
    let mut events = handle.events(before, context());
    let next = tokio::spawn(async move { events.next().await });
    store.empty_page_entered.notified().await;
    fixture.model.release.add_permits(1);
    completed(handle.outcome(&context()).await.unwrap());
    let terminal = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert!(terminal.last_event_seq > before);
    store.empty_page_release.add_permits(1);
    let delivered = next
        .await
        .unwrap()
        .expect("final durable event must not be lost")
        .unwrap();
    assert_eq!(delivered.event_type, "run.finished");
    assert_eq!(delivered.seq.get(), terminal.last_event_seq);
}

#[tokio::test]
async fn concurrent_duplicate_starts_share_one_run_and_one_model_attempt() {
    let fixture = Fixture::new(Response::Text, true);
    let agent = fixture.agent();
    let (first, second) = tokio::join!(
        agent.start(request("request"), context()),
        agent.start(request("request"), context())
    );
    let first = completed(first.unwrap());
    let second = completed(second.unwrap());
    assert_eq!(first.run_id(), second.run_id());
    fixture.model.entered.notified().await;
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    fixture.model.release.add_permits(1);
    let observer = context();
    let (first_outcome, second_outcome) =
        tokio::join!(first.outcome(&observer), second.outcome(&observer));
    assert_eq!(
        completed(first_outcome.unwrap()),
        completed(second_outcome.unwrap())
    );
}

#[tokio::test]
async fn adapter_panic_fails_and_repeated_unknown_tools_exhaust_without_tool_dispatch() {
    for (response, expected_status, expected_calls) in [
        (Response::Panic, RunStatus::Failed, 1),
        (Response::Tool, RunStatus::Exhausted, 1),
    ] {
        let fixture = Fixture::new(response, false);
        let agent = fixture.agent();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(outcome.result.status(), expected_status);
        assert_eq!(outcome.usage.model_calls, expected_calls);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(
            fixture.model.calls.load(Ordering::SeqCst),
            expected_calls as usize
        );
        assert!(
            fixture
                .store
                .load(&scope(), handle.run_id())
                .await
                .unwrap()
                .session
                .active_run_id
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_start_policy_denial_admits_no_run_and_calls_no_resolver_or_model() {
    let fixture = Fixture::new(Response::Text, false);
    fixture.policy.deny.store(3, Ordering::SeqCst);
    let agent = fixture.agent();
    assert_eq!(
        agent
            .start(request("request"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(
        fixture
            .store
            .find_request(&scope(), &id("session"), &id("request"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_run_replays_the_exact_committed_continuation_once_on_the_original_route() {
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let saved = fixture.store.load(&scope(), first.run_id()).await.unwrap();
    let opaque: Vec<_> = saved
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ContentBlock::ProviderOpaque {
                provider,
                route_digest,
                data_ref,
            } => Some((provider, route_digest, data_ref)),
            _ => None,
        })
        .collect();
    assert_eq!(opaque.len(), 1);
    assert_eq!(opaque[0].0, &id("fixture"));
    let record = fixture
        .store
        .read_record(&scope(), opaque[0].2)
        .await
        .unwrap();
    let expected: OpaqueContinuation = serde_json::from_value(record.value().clone()).unwrap();
    assert_eq!(
        expected.data(),
        &serde_json::json!({"signature":"fixture-signature"})
    );
    assert_eq!(expected.route_digest(), opaque[0].1);
    let second = fixture.started(&agent, "second").await;
    completed(second.outcome(&context()).await.unwrap());
    let requests = fixture.model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::Opaque { continuation } => Some(continuation),
            _ => None,
        })
        .collect();
    assert_eq!(replayed, vec![&expected]);
    assert_eq!(replayed[0].route_digest(), &requests[1].route.digest());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn changing_provider_for_the_same_session_does_not_forward_or_silently_discard_opaque_state()
{
    let fixture = Fixture::new(Response::WithContinuation, false);
    let agent = fixture.agent();
    let first = fixture.started(&agent, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let mut other = Fixture::new(Response::Text, false);
    other.store = fixture.store.clone();
    other.ids = fixture.ids.clone();
    other.clock = fixture.clock.clone();
    other.router = std::sync::Arc::new(Router::for_provider("different-provider"));
    let selected = other
        .router
        .snapshot
        .route_for_binding(&reference("route"))
        .unwrap();
    let mut port = Model::new(Response::Text, false);
    port.port_binding = ModelPortBinding {
        provider: selected.provider.clone(),
        adapter: selected.adapter.clone(),
        connection_ref: selected.connection_ref.clone(),
    };
    other.model = std::sync::Arc::new(port);
    let other_agent = other.agent();
    let second = other.started(&other_agent, "second").await;
    let outcome = completed(second.outcome(&context()).await.unwrap());
    assert!(
        matches!(outcome.result,OutcomeResult::Failed{failure} if failure.code==id("model_context_incompatible"))
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 0);
    assert!(other.model.requests.lock().unwrap().is_empty());
    // A clean session proves the second provider configuration is usable: the
    // earlier rejection is caused by incompatible continuation, not its binding.
    let mut clean = request("clean-request");
    clean.session_id = id("clean-session");
    let clean = completed(other_agent.start(clean, context()).await.unwrap());
    assert_eq!(
        completed(clean.outcome(&context()).await.unwrap())
            .result
            .status(),
        RunStatus::Succeeded
    );
    assert_eq!(other.model.calls.load(Ordering::SeqCst), 1);
    assert!(
        !other.model.requests.lock().unwrap()[0]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|content| matches!(content, ModelContent::Opaque { .. }))
    );
}

#[tokio::test]
async fn start_replay_treats_omitted_system_inputs_as_empty_instead_of_reusing_saved_values() {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.system_inputs = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: serde_json::json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    let agent = create_agent(profile(), bindings).unwrap();
    let mut supplied = context();
    supplied.data.system_inputs = Some(SystemInputs::new(JsonObject::from([(
        "workspace_id".into(),
        serde_json::json!("11111111-1111-4111-8111-111111111111"),
    )])));
    let first = completed(
        agent
            .start(request("with-inputs"), supplied.clone())
            .await
            .unwrap(),
    );
    completed(first.outcome(&context()).await.unwrap());
    let before = fixture.catalog.calls.load(Ordering::SeqCst);
    let omitted = agent
        .start(request("with-inputs"), context())
        .await
        .unwrap_err();
    assert!(matches!(
        omitted.code,
        ErrorCode::SystemInputsMismatch | ErrorCode::RequestConflict
    ));
    let same = completed(agent.start(request("with-inputs"), supplied).await.unwrap());
    assert_eq!(same.run_id(), first.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), before);

    let mut empty_request = request("empty-inputs");
    empty_request.session_id = id("empty-session");
    let empty = completed(agent.start(empty_request.clone(), context()).await.unwrap());
    completed(empty.outcome(&context()).await.unwrap());
    let mut explicit_empty = context();
    explicit_empty.data.system_inputs = Some(SystemInputs::default());
    let same_empty = completed(agent.start(empty_request, explicit_empty).await.unwrap());
    assert_eq!(same_empty.run_id(), empty.run_id());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn observer_cancellation_interrupts_pending_store_reads_without_cancelling_the_driver() {
    for operation in 0..3 {
        let fixture = Fixture::new(Response::Text, true);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        let agent = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        fixture.model.entered.notified().await;
        let observer = context();
        store
            .block_read
            .store(if operation == 1 { 2 } else { 1 }, Ordering::SeqCst);
        let waiting_handle = handle.clone();
        let waiting_agent = agent.clone();
        let waiting_context = observer.clone();
        let waiting = tokio::spawn(async move {
            match operation {
                0 => waiting_handle.outcome(&waiting_context).await.map(|_| ()),
                1 => waiting_handle
                    .events(0, waiting_context)
                    .next()
                    .await
                    .expect("observer must report cancellation")
                    .map(|_| ()),
                _ => waiting_agent
                    .get_run(waiting_handle.run_id(), &waiting_context)
                    .await
                    .map(|_| ()),
            }
        });
        store.read_entered.notified().await;
        observer.cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("cancel must interrupt the pending store read")
            .unwrap();
        assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
        fixture.model.release.add_permits(1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Succeeded
        );
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn initial_request_lookup_observes_cancellation_and_start_timeout_before_admission() {
    for cancel in [true, false] {
        let fixture = Fixture::new(Response::Text, false);
        let store = std::sync::Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            FinalCommitMode::PassThrough,
        ));
        store.block_read.store(3, Ordering::SeqCst);
        let mut bindings = fixture.bindings();
        bindings.state = store.clone();
        bindings.settings.start_timeout_ms = 30;
        let agent = create_agent(profile(), bindings).unwrap();
        let caller = context();
        let task_context = caller.clone();
        let start =
            tokio::spawn(async move { agent.start(request("request"), task_context).await });
        store.read_entered.notified().await;
        if cancel {
            caller.cancellation.cancel();
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), start)
            .await
            .expect("request lookup must be bounded by start control")
            .unwrap();
        assert_eq!(
            result.unwrap_err().code,
            if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::DeadlineExceeded
            }
        );
        assert!(
            fixture
                .store
                .find_request(&scope(), &id("session"), &id("request"))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(fixture.catalog.calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exhausting_a_retry_budget_preserves_the_partial_response_already_saved_for_this_step() {
    let fixture = Fixture::new(Response::TransportFailure, false);
    let mut profile = profile();
    profile.limits.max_model_calls = 1.try_into().unwrap();
    profile.limits.max_recovery_attempts = 1;
    let gate = std::sync::Arc::new(
        PolicyGate::new(fixture.policy.clone(), std::time::Duration::from_secs(1)).unwrap(),
    );
    let exchange = ModelExchange::new(fixture.model.clone(), gate)
        .with_route_inspector(fixture.inspector.clone(), std::time::Duration::from_secs(1))
        .unwrap()
        .with_retry_policy(ModelRetryPolicy {
            max_retries: 1,
            backoff_ms: 0,
        });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = std::sync::Arc::new(exchange);
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::ModelCalls
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "candidate answer".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 1);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    let saved = fixture
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.outcome, Some(outcome));
    assert!(saved.model_ledger[0].response_ref.is_some());
}

#[tokio::test]
async fn another_session_finishes_while_the_first_run_waits_on_model_io() {
    use std::sync::{Arc, atomic::AtomicUsize};
    use std::time::Duration;
    struct Lanes {
        slow: Arc<support::Model>,
        fast: Arc<support::Model>,
        calls: AtomicUsize,
    }
    impl ModelPort for Lanes {
        fn binding(&self) -> ModelPortBinding {
            self.slow.binding()
        }
        fn generate<'a>(
            &'a self,
            request: &'a ModelRequest,
            context: &'a ModelCallContext,
        ) -> PortStream<'a, ModelEvent> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.slow.generate(request, context)
            } else {
                self.fast.generate(request, context)
            }
        }
    }
    let fixture = Fixture::new(Response::Text, false);
    let lanes = Arc::new(Lanes {
        slow: Arc::new(support::Model::new(Response::Text, true)),
        fast: Arc::new(support::Model::new(Response::Text, false)),
        calls: AtomicUsize::new(0),
    });
    let mut bindings = fixture.bindings();
    bindings.model_exchange = Arc::new(
        ModelExchange::new(lanes.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let agent = create_agent(profile(), bindings).unwrap();
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut slow_request = request("slow");
    slow_request.session_id = id("slow-session");
    let slow = completed(agent.start(slow_request, context()).await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), lanes.slow.entered.notified())
        .await
        .unwrap();
    assert_eq!(lanes.slow.release.available_permits(), 0);
    let mut fast_request = request("fast");
    fast_request.session_id = id("fast-session");
    let fast = completed(agent.start(fast_request, context()).await.unwrap());
    let result = tokio::time::timeout(Duration::from_secs(2), fast.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(
        fixture
            .store
            .load(&scope(), slow.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Running
    );
    assert_eq!(lanes.slow.release.available_permits(), 0);
    lanes.slow.release.add_permits(1);
    let result = tokio::time::timeout(Duration::from_secs(2), slow.outcome(&context()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed(result).result.status(), RunStatus::Succeeded);
    assert_eq!(lanes.calls.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("slow-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    assert!(
        fixture
            .store
            .load_session(&scope(), &id("fast-session"))
            .await
            .unwrap()
            .active_run_id
            .is_none()
    );
    drop(slow);
    drop(fast);
    drop(agent);
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            > baseline
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn saved_submission_survives_catalog_outage_but_not_revoked_authorization() {
    struct Offline(std::sync::atomic::AtomicUsize);
    impl ProfileResolver for Offline {
        fn resolve<'a>(
            &'a self,
            _: &'a ComponentRef,
            _: &'a Scope,
        ) -> PortFuture<'a, ComponentMetadata> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ContractError::new(
                    ErrorCode::ComponentUnavailable,
                    "offline.catalog",
                ))
            })
        }
    }
    let fixture = Fixture::new(Response::Text, false);
    let first_agent = fixture.agent();
    let first = fixture.started(&first_agent, "original").await;
    completed(first.outcome(&context()).await.unwrap());
    let offline = std::sync::Arc::new(Offline(std::sync::atomic::AtomicUsize::new(0)));
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = offline.clone();
    let restarted = create_agent(profile(), bindings).unwrap();
    let replay = completed(
        restarted
            .start(request("original"), context())
            .await
            .unwrap(),
    );
    assert_eq!(replay.run_id(), first.run_id());
    assert_eq!(offline.0.load(Ordering::SeqCst), 0);
    fixture.policy.deny.store(3, Ordering::SeqCst);
    assert_eq!(
        restarted
            .start(request("original"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(offline.0.load(Ordering::SeqCst), 0);
    fixture.policy.deny.store(0, Ordering::SeqCst);
    assert_eq!(
        restarted
            .start(request("new-offline"), context())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ComponentUnavailable
    );
    assert!(offline.0.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn profile_and_run_options_reach_the_model_with_pinned_origins_and_output_caps() {
    for (run_effort, run_cap, expected_effort, expected_source, expected_cap) in [
        (None, None, "low", ModelOptionSource::Profile, 80),
        (Some("high"), Some(40), "high", ModelOptionSource::Run, 40),
        (Some("high"), Some(120), "high", ModelOptionSource::Run, 80),
    ] {
        let fixture = Fixture::new(Response::Text, false);
        let mut profile = profile();
        profile
            .model_options
            .insert("effort".into(), serde_json::json!("low"));
        profile.limits.max_output_tokens = Some(80.try_into().unwrap());
        let agent = create_agent(profile, fixture.bindings()).unwrap();
        let mut input = request("options");
        if let Some(effort) = run_effort {
            input
                .model_options
                .insert("effort".into(), serde_json::json!(effort));
        }
        input.max_output_tokens = run_cap.map(|cap: u64| cap.try_into().unwrap());
        let handle = completed(agent.start(input, context()).await.unwrap());
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(outcome.result.status(), RunStatus::Succeeded);
        {
            let calls = fixture.model.requests.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].options["effort"],
                serde_json::json!(expected_effort)
            );
            assert_eq!(calls[0].max_output_tokens.get(), expected_cap);
        }
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        let config = saved.snapshot.model_ledger[0]
            .configuration
            .as_ref()
            .unwrap();
        assert_eq!(config.sources["effort"], expected_source);
        assert_eq!(
            config.effective["effort"],
            serde_json::json!(expected_effort)
        );
        assert_eq!(config.max_output_tokens.get(), expected_cap);
    }
}

#[tokio::test]
async fn invalid_tool_rounds_have_a_separate_finite_repair_budget() {
    for capacity in [0, 1, 2] {
        let fixture = Fixture::new(Response::Tool, false);
        let mut profile = profile();
        profile.limits.max_repair_attempts = capacity;
        let agent = create_agent(profile, fixture.bindings()).unwrap();
        let handle = fixture.started(&agent, "repair").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        assert_eq!(
            outcome.result,
            OutcomeResult::Exhausted {
                budget: BudgetKind::RepairAttempts
            }
        );
        assert_eq!(outcome.usage.model_calls, capacity + 1);
        assert_eq!(outcome.usage.repair_attempts, capacity);
        assert_eq!(outcome.usage.tool_attempts, 0);
        assert_eq!(outcome.usage.recovery_attempts, 0);
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        let rounds: std::collections::BTreeSet<_> = saved
            .snapshot
            .reservations
            .iter()
            .filter_map(|reservation| {
                if let ReservationKind::ToolRepair { model_request_id } = &reservation.kind {
                    Some(model_request_id)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(rounds.len(), capacity as usize);
    }
}

#[tokio::test]
async fn legacy_terminal_records_keep_read_only_start_outcome_and_event_replay() {
    let fixture = Fixture::new(Response::Text, false);
    let original = fixture.started(&fixture.agent(), "legacy-read").await;
    let outcome = completed(original.outcome(&context()).await.unwrap());
    let mut image =
        serde_json::to_value(fixture.store.export_checkpoint(&scope()).unwrap()).unwrap();
    image["schema_version"] = serde_json::json!("wickle.state-store.v1");
    image.as_object_mut().unwrap().remove("executions");
    image.as_object_mut().unwrap().remove("legacy_runs");
    let checkpoint =
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .unwrap();
    let restored = std::sync::Arc::new(MemoryStateStore::from_checkpoint(checkpoint));
    let mut bindings = fixture.bindings();
    bindings.state = restored.clone();
    let agent = create_agent(profile(), bindings).unwrap();
    let handle = fixture.started(&agent, "legacy-read").await;
    assert_eq!(
        completed(handle.outcome(&context()).await.unwrap()),
        outcome
    );
    let mut events = handle.events(0, context());
    let mut last = None;
    while let Some(event) = events.next().await {
        last = Some(event.unwrap().event_type);
    }
    assert_eq!(last, Some("run.finished"));
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        restored
            .read_execution(&scope(), handle.run_id())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CapabilityUnsupported
    );
}
