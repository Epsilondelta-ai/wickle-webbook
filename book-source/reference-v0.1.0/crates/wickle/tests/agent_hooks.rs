//! Lifecycle hooks preserve authority, original arguments, checkpoint reuse, and committed effects.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod support;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use support::*;
use wickle::*;

#[tokio::test]
async fn serial_transform_order_uses_priority_then_id_and_keeps_original_model_arguments() {
    let mut fixture = Fixture::new();
    fixture.add("z", HookPosition::BeforeTool, Behavior::Append, 0, true);
    fixture.add("a", HookPosition::BeforeTool, Behavior::Append, 0, true);
    fixture.add(
        "first",
        HookPosition::BeforeTool,
        Behavior::Append,
        -1,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec!["first", "a", "z", "first", "a", "z", "first", "a", "z"]
    );
    let saved = fixture.saved(&handle).await;
    for (index, name) in ["before", "target", "after"].iter().enumerate() {
        let entry = &saved.snapshot.tool_ledger[index];
        assert_eq!(entry.call.model_inputs, object(json!({"query":name})));
        let record = fixture
            .base
            .base
            .store
            .read_record(&scope(), entry.call.bound_input_ref.as_ref().unwrap())
            .await
            .unwrap();
        let compiled = &fixture.base.registry.get(&id(name)).unwrap().compiled;
        let bound = BoundToolInput::restore(
            &record,
            compiled,
            &scope(),
            handle.run_id(),
            &entry.call,
            saved.snapshot.system_inputs.as_ref(),
        )
        .unwrap();
        assert_eq!(
            bound.original_model_inputs(),
            &object(json!({"query":name}))
        );
        assert_eq!(
            bound.effective_model_inputs(),
            &object(json!({"query":format!("{name}|first|a|z")}))
        );
        assert_eq!(
            fixture.base.tools[index].seen.lock().unwrap()[0].arguments["query"],
            json!(format!("{name}|first|a|z"))
        );
    }
    assert_eq!(
        fixture.base.tools[1].seen.lock().unwrap()[0].arguments["workspace_id"],
        json!(WORKSPACE)
    );
    let HookInput::BeforeTool {
        original_model_inputs,
        model_inputs,
        ..
    } = &fixture.hooks[0].seen.lock().unwrap()[0].0
    else {
        panic!("before_tool input required")
    };
    assert_eq!(original_model_inputs, &object(json!({"query":"before"})));
    assert_eq!(model_inputs, &object(json!({"query":"before|first|a"})));
    let requests = fixture.base.model.requests.lock().unwrap().clone();
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            ModelContent::ToolCall { arguments, .. } => Some(arguments),
            _ => None,
        })
        .collect();
    assert_eq!(
        replayed,
        vec![
            &object(json!({"query":"before"})),
            &object(json!({"query":"target"})),
            &object(json!({"query":"after"}))
        ]
    );
}

#[tokio::test]
async fn hidden_system_fields_and_invalid_public_values_are_rejected_before_binding_or_execution() {
    for behavior in [Behavior::Hidden, Behavior::InvalidValue] {
        let mut fixture = Fixture::new();
        fixture.add(
            "invalid-transform",
            HookPosition::BeforeTool,
            behavior,
            0,
            true,
        );
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        let result = fixture.outcome(&handle).await;
        assert_ne!(result.result.status(), RunStatus::Succeeded);
        assert_eq!(fixture.base.resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
        assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 1);
        assert!(fixture.policy.seen.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn hook_continuation_does_not_override_host_denial_or_unavailable_policy() {
    for unavailable in [false, true] {
        let mut fixture = Fixture::new();
        fixture.add(
            "transform",
            HookPosition::BeforeTool,
            Behavior::Append,
            0,
            true,
        );
        fixture.policy.deny.store(!unavailable, Ordering::SeqCst);
        fixture.policy.fail.store(unavailable, Ordering::SeqCst);
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        fixture.outcome(&handle).await;
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
        assert!(!fixture.policy.seen.lock().unwrap().is_empty());
        assert!(fixture.policy.seen.lock().unwrap().iter().all(|input| {
            input.execution_args()["query"]
                .as_str()
                .unwrap()
                .ends_with("|transform")
        }));
    }
}

#[tokio::test]
async fn a_hook_denial_survives_following_continuation_and_prevents_all_effects() {
    let mut fixture = Fixture::new();
    let deny = fixture.add("deny", HookPosition::BeforeTool, Behavior::Deny, 0, true);
    fixture.add(
        "continue",
        HookPosition::BeforeTool,
        Behavior::Append,
        1,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    fixture.outcome(&handle).await;
    assert_eq!(deny.calls.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
    assert!(fixture.saved(&handle).await.snapshot.tool_ledger.iter().all(|entry| matches!(&entry.state, ToolCallState::Settled { result } if result.status == ToolResultStatus::Denied && result.effect == ToolEffect::NotApplied)));
}

fn context_data(request: &ModelRequest) -> Vec<(ModelRole, &Value)> {
    request
        .messages
        .iter()
        .flat_map(|message| {
            message.content.iter().filter_map(|content| match content {
                ModelContent::Json { value } if value["kind"] == "context_data" => {
                    Some((message.role, value))
                }
                _ => None,
            })
        })
        .collect()
}

#[tokio::test]
async fn added_context_preserves_core_provenance_lifetime_prefix_and_original_request() {
    let mut fixture = Fixture::new();
    let run = fixture.add(
        "run-data",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    let step = fixture.add(
        "step-data",
        HookPosition::BeforeModel,
        Behavior::Context,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(step.calls.load(Ordering::SeqCst), 2);
    let saved = fixture.saved(&handle).await;
    let requests = fixture.base.model.requests.lock().unwrap().clone();
    assert_eq!(requests[0].messages[..2], requests[1].messages[..2]);
    assert!(
        requests[0].messages[..2]
            .iter()
            .all(|message| message.role == ModelRole::System)
    );
    let first = context_data(&requests[0]);
    let second = context_data(&requests[1]);
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    for (role, item) in first.iter().chain(&second) {
        assert_eq!(*role, ModelRole::User);
        assert_eq!(item["origin"], json!("hook"));
    }
    let run_item = |items: &[(ModelRole, &Value)]| {
        items
            .iter()
            .find(|(_, item)| item["source_ref"]["id"] == "run-data")
            .unwrap()
            .1["item_id"]
            .clone()
    };
    let step_item = |items: &[(ModelRole, &Value)]| {
        items
            .iter()
            .find(|(_, item)| item["source_ref"]["id"] == "step-data")
            .unwrap()
            .1["item_id"]
            .clone()
    };
    assert_eq!(run_item(&first), run_item(&second));
    assert_ne!(step_item(&first), step_item(&second));
    assert_eq!(saved.snapshot.request, request("request"));
    for application in &saved.snapshot.hook_applications {
        let record = fixture
            .base
            .base
            .store
            .read_record(&scope(), &application.result_ref)
            .await
            .unwrap();
        let result: HookApplicationRecord = serde_json::from_value(record.value().clone()).unwrap();
        for item in &result.context_items {
            assert_eq!(item.scope, scope());
            assert_eq!(item.origin, ContextOrigin::Hook);
            assert_eq!(item.source_ref, application.hook);
            assert!(
                matches!((&application.target, &item.lifetime),
                (HookTarget::BeforeRun, ContextLifetime::Run { run_id }) if run_id == handle.run_id())
                    || matches!((&application.target, &item.lifetime), (HookTarget::BeforeModel { model_step_id }, ContextLifetime::Step { run_id, model_step_id: item_step }) if run_id == handle.run_id() && model_step_id == item_step)
            );
        }
    }
}

#[tokio::test]
async fn approval_resume_reuses_the_stored_transform_and_original_resolver_binding() {
    let mut fixture = Fixture::new();
    let run = fixture.add(
        "run-data",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    let transform = fixture.add(
        "append",
        HookPosition::BeforeTool,
        Behavior::Append,
        0,
        true,
    );
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    let before = fixture.saved(&original).await;
    assert_eq!(transform.calls.load(Ordering::SeqCst), 2);
    *transform.suffix.lock().unwrap() = "CHANGED".into();
    *fixture.base.resolver.value.lock().unwrap() = ResolvedSystemInput {
        value: json!(resume_support::CHANGED_RECORD),
        revision: id("new-record"),
    };
    let resumed = completed(
        agent
            .resume(fixture.base.approve(&original, "approve").await, context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(transform.calls.load(Ordering::SeqCst), 3); // Only the previously unprocessed final call is new.
    assert_eq!(fixture.base.resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.base.tools[1].seen.lock().unwrap()[0].arguments,
        object(
            json!({"query":"target|append","workspace_id":WORKSPACE,"record_id":resume_support::RECORD})
        )
    );
    assert_eq!(
        fixture.base.tools[2].seen.lock().unwrap()[0].arguments,
        object(json!({"query":"after|CHANGED"}))
    );
    let saved = fixture.saved(&resumed).await;
    assert!(
        saved
            .snapshot
            .hook_applications
            .starts_with(&before.snapshot.hook_applications)
    );
    assert_eq!(
        saved.snapshot.tool_ledger[1].call,
        before.snapshot.tool_ledger[1].call
    );
}

#[tokio::test]
async fn transport_retry_reuses_before_model_for_the_same_logical_step() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "per-step",
        HookPosition::BeforeModel,
        Behavior::Context,
        0,
        true,
    );
    fixture.model.fail_first.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let result = fixture.outcome(&handle).await;
    assert_eq!(result.result.status(), RunStatus::Succeeded);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    let saved = fixture.saved(&handle).await;
    assert_eq!(
        saved.snapshot.model_ledger[0].model_step_id,
        saved.snapshot.model_ledger[1].model_step_id
    );
    assert_ne!(
        saved.snapshot.model_ledger[1].model_step_id,
        saved.snapshot.model_ledger[2].model_step_id
    );
}

#[tokio::test(start_paused = true)]
async fn optional_before_run_callback_failures_continue_but_required_failures_stop_execution() {
    for behavior in [Behavior::Error, Behavior::Pending, Behavior::Panic] {
        for required in [false, true] {
            let mut fixture = Fixture::new();
            let hook = fixture.add("prepare", HookPosition::BeforeRun, behavior, 0, required);
            let agent = fixture.agent();
            let handle = fixture.started(&agent).await;
            let result = fixture.outcome(&handle).await;
            assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
            if required {
                assert_eq!(result.result.status(), RunStatus::Failed);
                assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
                assert!(
                    fixture
                        .base
                        .tools
                        .iter()
                        .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
                );
            } else {
                assert_eq!(result.result.status(), RunStatus::Succeeded);
                let saved = fixture.saved(&handle).await;
                let application = saved.snapshot.hook_applications.first().unwrap();
                let record = fixture
                    .base
                    .base
                    .store
                    .read_record(&scope(), &application.result_ref)
                    .await
                    .unwrap();
                let result: HookApplicationRecord =
                    serde_json::from_value(record.value().clone()).unwrap();
                assert!(result.failure.is_some());
                assert!(result.output.is_none());
            }
        }
    }
}

#[tokio::test]
async fn invalid_optional_patches_and_oversized_required_context_never_reach_the_model() {
    for behavior in [Behavior::WrongVariant, Behavior::Oversized] {
        let mut fixture = Fixture::new();
        fixture.add(
            "invalid-context",
            HookPosition::BeforeRun,
            behavior,
            0,
            false,
        );
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Failed
        );
        assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_failure_after_commit_never_changes_outcomes_or_repeats_tools() {
    for behavior in [Behavior::Error, Behavior::Panic, Behavior::Pending] {
        let mut fixture = Fixture::new();
        let tool_observer = fixture.add("observe-tool", HookPosition::AfterTool, behavior, 0, true);
        let run_observer = fixture.add("observe-run", HookPosition::AfterRun, behavior, 0, true);
        let agent = fixture.agent();
        let handle = fixture.started(&agent).await;
        let result = fixture.outcome(&handle).await;
        assert_eq!(result.result.status(), RunStatus::Succeeded);
        let before_reports = fixture.saved(&handle).await;
        let view = observations(&handle, 4).await;
        assert!(view.local_error.is_none());
        assert_eq!(view.reports.len(), 4);
        assert!(
            view.reports
                .iter()
                .all(|report| matches!(report.status, HookObservationStatus::Failed { .. }))
        );
        assert_eq!(
            fixture.saved(&handle).await.snapshot,
            before_reports.snapshot
        );
        assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 3);
        assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
        let replay = fixture.started(&agent).await;
        assert_eq!(fixture.outcome(&replay).await, result);
        assert_eq!(fixture.base.tools[1].calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.base.tools[1].applied.load(Ordering::SeqCst), 1);
        assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 3);
        assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn waiting_does_not_invoke_after_run_and_cancellation_stops_a_pending_before_run() {
    let mut fixture = Fixture::new();
    let observer = fixture.add(
        "after-run",
        HookPosition::AfterRun,
        Behavior::Observe,
        0,
        true,
    );
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Waiting
    );
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    let resumed = completed(
        agent
            .resume(fixture.base.approve(&handle, "approve").await, context())
            .await
            .unwrap(),
    );
    fixture.outcome(&resumed).await;
    observations(&resumed, 1).await;
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);

    let mut fixture = Fixture::new();
    let hook = fixture.add("blocked", HookPosition::BeforeRun, Behavior::Pause, 0, true);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    gate(&hook.entered).await;
    completed(handle.cancel(id("stop-hook"), &context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Cancelled
    );
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn revoked_hook_permission_prevents_the_callback_and_any_model_dispatch() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "prepare",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    fixture.policy.deny_hooks.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn observer_report_storage_failure_is_separate_from_the_successful_run_outcome() {
    let mut fixture = Fixture::new();
    let observer = fixture.add(
        "after-run",
        HookPosition::AfterRun,
        Behavior::Observe,
        0,
        true,
    );
    fixture.store.observer_failure.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 1).await;
    assert_eq!(
        view.local_error.as_ref().unwrap().code,
        ErrorCode::PersistenceUnavailable
    );
    assert!(view.reports.is_empty());
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
    let replay = fixture.started(&agent).await;
    assert_eq!(fixture.outcome(&replay).await, outcome);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.base.tools[1].applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn failed_transform_persistence_stops_execution_before_a_model_or_tool_can_use_it() {
    let mut fixture = Fixture::new();
    let hook = fixture.add(
        "prepare",
        HookPosition::BeforeRun,
        Behavior::Context,
        0,
        true,
    );
    fixture.store.transform_failure.store(1, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    assert_eq!(
        handle.outcome(&context()).await.unwrap_err().code,
        ErrorCode::PersistenceUnavailable
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .saved(&handle)
            .await
            .snapshot
            .hook_applications
            .is_empty()
    );
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn transform_replay_distinguishes_unsaved_results_from_lost_commit_acknowledgements() {
    for fail_mode in [1, 2] {
        let mut fixture = Fixture::new();
        let hook = fixture.add(
            "append",
            HookPosition::BeforeTool,
            Behavior::Append,
            0,
            true,
        );
        fixture
            .store
            .transform_failure
            .store(fail_mode, Ordering::SeqCst);
        let bindings = fixture.bindings();
        let runtime = bindings.hooks.as_ref().unwrap().clone();
        let agent = create_agent(fixture.profile(), bindings).unwrap();
        let handle = fixture.started(&agent).await;
        assert_eq!(
            handle.outcome(&context()).await.unwrap_err().code,
            ErrorCode::PersistenceUnavailable
        );
        let saved = fixture.saved(&handle).await;
        assert_eq!(
            saved.snapshot.hook_applications.len(),
            usize::from(fail_mode == 2)
        );
        assert_eq!(fixture.base.tools[0].calls.load(Ordering::SeqCst), 0);
        let call = &saved.snapshot.tool_ledger[0].call;
        let compiled = &fixture.base.registry.get(&id("before")).unwrap().compiled;
        let input = HookInput::BeforeTool {
            tool: compiled.to_model_tool(),
            descriptor_digest: compiled.descriptor_digest().clone(),
            compiled_digest: compiled.digest().clone(),
            original_model_inputs: call.model_inputs.clone(),
            model_inputs: call.model_inputs.clone(),
        };
        let lease = fixture
            .store
            .acquire_lease(
                &scope(),
                handle.run_id(),
                &id("replay-worker"),
                fixture.base.base.clock.now().unwrap().utc_ms,
                1000,
            )
            .await
            .unwrap();
        let budget = RunBudget::attach(
            fixture.store.clone(),
            fixture.base.base.clock.clone(),
            fixture.base.base.ids.clone(),
            scope(),
            handle.run_id().clone(),
            lease,
            Default::default(),
        )
        .await
        .unwrap();
        let replay = runtime
            .transform(
                HookTarget::BeforeTool {
                    call_id: call.call_id.clone(),
                },
                input,
                &context(),
                &budget,
            )
            .await
            .unwrap();
        assert_eq!(
            replay.model_inputs,
            Some(object(json!({"query":"before|append"})))
        );
        assert_eq!(
            hook.calls.load(Ordering::SeqCst),
            if fail_mode == 1 { 2 } else { 1 }
        );
        assert_eq!(
            fixture
                .saved(&handle)
                .await
                .snapshot
                .hook_applications
                .len(),
            1
        );
        assert!(
            fixture
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn after_tool_observers_use_remaining_run_time_and_expired_calls_do_not_enter_them() {
    let mut fixture = Fixture::new();
    fixture.base.profile.limits.max_elapsed_ms = 5.try_into().unwrap();
    let tool_observer = fixture.add(
        "tool-watch",
        HookPosition::AfterTool,
        Behavior::Pending,
        0,
        true,
    );
    let run_observer = fixture.add(
        "run-watch",
        HookPosition::AfterRun,
        Behavior::Pending,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::Elapsed
        }
    );
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 4).await;
    assert!(view.local_error.is_none());
    assert_eq!(tool_observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(run_observer.calls.load(Ordering::SeqCst), 1);
    let tool_time = tool_observer.entered_at.lock().unwrap()[0];
    assert!(
        tool_observer.seen.lock().unwrap()[0].1.deadline - tool_time
            <= std::time::Duration::from_millis(5)
    );
    let run_time = run_observer.entered_at.lock().unwrap()[0];
    assert!(run_time > tool_time);
    assert_eq!(
        run_observer.seen.lock().unwrap()[0].1.deadline - run_time,
        std::time::Duration::from_millis(20)
    );
    assert_eq!(fixture.base.tools[0].calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.base.tools[1].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.base.tools[2].calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
}

#[tokio::test(start_paused = true)]
async fn after_run_uses_a_fresh_bounded_cleanup_context_after_explicit_cancellation() {
    let mut fixture = Fixture::new();
    let before = fixture.add(
        "waiting-prepare",
        HookPosition::BeforeRun,
        Behavior::Pause,
        0,
        true,
    );
    let after = fixture.add(
        "after-cancel",
        HookPosition::AfterRun,
        Behavior::Pending,
        0,
        true,
    );
    let agent = fixture.agent();
    let handle = fixture.started(&agent).await;
    gate(&before.entered).await;
    completed(
        handle
            .cancel(id("cancel-before-model"), &context())
            .await
            .unwrap(),
    );
    let outcome = fixture.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Cancelled);
    let saved = fixture.saved(&handle).await;
    let view = observations(&handle, 1).await;
    assert!(view.local_error.is_none());
    assert_eq!(after.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        view.reports[0].status,
        HookObservationStatus::Failed { .. }
    ));
    let entered = after.entered_at.lock().unwrap()[0];
    assert_eq!(
        after.seen.lock().unwrap()[0].1.deadline - entered,
        std::time::Duration::from_millis(20)
    );
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert_eq!(fixture.outcome(&handle).await, outcome);
    assert_eq!(fixture.model.attempts.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .base
            .tools
            .iter()
            .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
    );
}
