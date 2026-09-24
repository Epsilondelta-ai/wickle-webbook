//! Core-owned attempts, policy, inspection and fallback through real routing and dispatch.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use serde_json::json;
use wickle::*;
use wickle_model_router::PolicyModelRouter;

#[allow(dead_code)]
#[path = "../../wickle/tests/support/mod.rs"]
mod core;
#[path = "support/routed.rs"]
mod support;
use core::{id, scope};
use support::*;

#[tokio::test]
async fn retry_and_fallback_share_saved_budgets_and_keep_exact_accounts_and_inspection_evidence() {
    let fixture = Fixture::new(
        vec![
            Reply::Fail(ModelFailureKind::RateLimited),
            Reply::Fail(ModelFailureKind::RateLimited),
        ],
        vec![Reply::Complete],
    )
    .await;
    let input = fixture.input("step");
    let result = fixture
        .exchange(1)
        .generate_routed(
            &fixture.router,
            &input,
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap();
    let response = completed(result);
    assert_eq!(fixture.call_counts(), (2, 1));
    assert_eq!(
        response.route_digest,
        fixture.second.calls.lock().unwrap()[0].route.digest()
    );
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 3);
    assert_eq!(saved.usage.recovery_attempts, 2);
    assert_eq!(saved.model_ledger.len(), 3);
    assert_eq!(
        saved.model_ledger[0].configuration,
        saved.model_ledger[1].configuration
    );
    for attempt in &saved.model_ledger {
        let config = attempt.configuration.as_ref().unwrap();
        assert_eq!(config.effective, options());
        assert_eq!(config.sources["effort"], ModelOptionSource::Run);
        assert_eq!(config.model_schema_revision, id("capabilities"));
    }

    assert_eq!(
        saved
            .model_ledger
            .iter()
            .map(|entry| entry.selection_reason.as_str())
            .collect::<Vec<_>>(),
        ["initial_route", "same_route_retry", "fallback_rate_limited"]
    );
    assert_ne!(
        saved.model_ledger[0].attempt_id,
        saved.model_ledger[1].attempt_id
    );
    assert_ne!(
        saved.model_ledger[1].attempt_id,
        saved.model_ledger[2].attempt_id
    );
    for entry in &saved.model_ledger {
        let reference = entry
            .inspection_ref
            .as_ref()
            .expect("inspection evidence must be persisted");
        let record = fixture
            .store
            .read_record(&scope(), reference)
            .await
            .unwrap();
        let observed: ModelRouteObservation =
            serde_json::from_value(record.value().clone()).unwrap();
        observed
            .validate(&entry.route, VersionPolicy::RequirePinned)
            .unwrap();
        assert_eq!(
            observed.model_version.as_ref(),
            Some(&entry.route.model_version)
        );
        assert!(entry.reported_model_version.is_none());
        let policies = fixture.policy.calls.lock().unwrap();
        assert!(
            policies
                .iter()
                .filter(|(route, _)| route == &entry.route)
                .count()
                >= 2
        );
    }
    for request in fixture
        .first
        .calls
        .lock()
        .unwrap()
        .iter()
        .chain(fixture.second.calls.lock().unwrap().iter())
    {
        assert_eq!(request.options, input.routing.options);
        assert_eq!(request.messages[0].role, ModelRole::User);
    }
    assert_eq!(
        saved.model_ledger[0].route.connection_ref.id,
        id("primary-account")
    );
    assert_eq!(
        saved.model_ledger[2].route.connection_ref.id,
        id("fallback-account")
    );
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn denied_fallback_is_neither_projected_nor_inspected_nor_dispatched() {
    let fixture = Fixture::new(
        vec![Reply::Fail(ModelFailureKind::RateLimited)],
        vec![Reply::Complete],
    )
    .await;
    *fixture.policy.denied_provider.lock().unwrap() = Some(id("provider-b"));
    let error = fixture
        .exchange(0)
        .generate_routed(
            &fixture.router,
            &fixture.input("step"),
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::AccessDenied);
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.projector.calls.lock().unwrap().len(), 1);
    let requests = fixture.policy.calls.lock().unwrap();
    let denied = requests
        .iter()
        .find(|(route, _)| route.provider == id("provider-b"))
        .unwrap();
    assert_eq!(denied.0.target["region"], json!("fallback-region"));
    assert_eq!(denied.0.connection_ref.id, id("fallback-account"));
}

#[tokio::test]
async fn completed_steps_reuse_saved_response_but_recheck_permission_and_projection_identity() {
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let input = fixture.input("step");
    let exchange = fixture.exchange(0);
    let first = completed(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    let revision = fixture.saved().await.revision;
    let second = completed(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    assert_eq!(first, second);
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 1);
    assert_eq!(fixture.saved().await.revision, revision);
    *fixture.projector.mode.lock().unwrap() = Projection::DifferentContent;
    fixture.first.panic_compiler.store(true, Ordering::SeqCst);
    // A completed preparation is restored, not regenerated with changed Host code.
    let replay = completed(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    assert_eq!(replay, first);
    assert_eq!(fixture.first.compiler_lookups.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.saved().await.revision, revision);
    assert_eq!(fixture.call_counts(), (1, 0));
    *fixture.projector.mode.lock().unwrap() = Projection::Valid;
    *fixture.policy.denied_provider.lock().unwrap() = Some(id("provider-a"));
    assert_eq!(
        exchange
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.call_counts(), (1, 0));
}

#[tokio::test]
async fn a_pinned_routing_snapshot_cannot_be_replaced_by_a_new_policy_or_catalog() {
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let exchange = fixture.exchange(0);
    exchange
        .generate_routed(
            &fixture.router,
            &fixture.input("step"),
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap();
    for catalog_change in [false, true] {
        let mut catalog = fixture.snapshot.catalog().clone();
        let mut policy = fixture.snapshot.policy().clone();
        if catalog_change {
            catalog.revision = id("catalog-2");
        } else {
            policy.revision = id("policy-2");
        }
        let replacement =
            PolicyModelRouter::new(RoutingSnapshot::new(catalog, policy).unwrap()).unwrap();
        assert_eq!(
            exchange
                .generate_routed(
                    &replacement,
                    &fixture.input("another-step"),
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await
                )
                .await
                .unwrap_err()
                .code,
            ErrorCode::ModelRoutingMismatch
        );
    }
    let saved = fixture.saved().await;
    let reference = saved.routing_snapshot_ref.unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &reference)
        .await
        .unwrap();
    let restored =
        RoutingSnapshot::restore(&record.value().to_string(), &scope(), &reference.digest).unwrap();
    assert_eq!(restored.digest(), fixture.snapshot.digest());
    assert_eq!(fixture.call_counts(), (1, 0));
}

#[tokio::test]
async fn malformed_route_projection_options_tokens_and_foreign_opaque_stop_before_model_calls() {
    for mode in [
        Projection::WrongRoute,
        Projection::WrongOptions,
        Projection::TooManyTokens,
        Projection::OldOpaque,
    ] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        *fixture.projector.mode.lock().unwrap() = mode;
        assert!(
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.call_counts(), (0, 0));
        assert!(fixture.inspector.calls.lock().unwrap().is_empty());
        assert_eq!(fixture.saved().await.usage.model_calls, 0);
    }
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let mut input = fixture.input("step");
    input.routing.scope.workspace_id = id("foreign");
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert!(fixture.projector.calls.lock().unwrap().is_empty());
    assert_eq!(fixture.call_counts(), (0, 0));
}

#[tokio::test]
async fn actual_tools_and_json_output_require_capabilities_even_when_the_host_requested_only_text()
{
    for (mode, feature) in [
        (Projection::UsesTools, "tool_calling"),
        (Projection::UsesJson, "json_output"),
    ] {
        for supported in [false, true] {
            let mut fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
            if supported {
                fixture.enable_feature(feature);
            }
            *fixture.projector.mode.lock().unwrap() = mode;
            let input = fixture.input("step");
            assert_eq!(
                input.routing.required_capabilities,
                [id("text")].into_iter().collect()
            );
            let result = fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &input,
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await,
                )
                .await;
            if supported {
                completed(result.unwrap());
                assert_eq!(fixture.call_counts(), (1, 0));
            } else {
                assert!(result.is_err());
                assert_eq!(fixture.call_counts(), (0, 0));
                assert!(fixture.inspector.calls.lock().unwrap().is_empty());
            }
        }
    }
}

#[tokio::test]
async fn routed_invocations_need_valid_inspection_records_in_both_commits_and_restored_checkpoints()
{
    // The valid branch prevents unrelated checkpoint-shape errors from satisfying the rejection cases.
    for corruption in [
        "valid",
        "missing",
        "unverified",
        "missing-step",
        "foreign-step",
    ] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap();
        let attempt = fixture
            .budget()
            .await
            .reserve(ReservationKind::Model {
                purpose: ModelPurpose::Agent,
            })
            .await
            .unwrap();
        let saved = fixture.saved().await;
        let mut invocation = saved.model_ledger[0].clone();
        invocation.attempt_id = attempt.attempt_id;
        invocation.model_step_id = id("new-step");
        invocation.state = ModelAttemptState::Reserved {};
        invocation.response_ref = None;
        invocation.provider_request_id = None;
        invocation.reported_model_id = None;
        invocation.reported_model_version = None;
        invocation.usage = None;
        let mut records = vec![];
        if corruption != "missing-step" {
            let mut input = fixture.input("new-step");
            if corruption == "foreign-step" {
                input.routing.scope.tenant_id = id("another-tenant");
            }
            records.push(ProtectedRecord::new(id(&format!("model-step-{}",canonical_digest(&json!([saved.run_id,"new-step"])))),1,json!({"schema_version":"wickle.model-step.v2","run_id":saved.run_id,"through_sequence":1,"input":input})));
        }
        if corruption == "missing" {
            invocation.inspection_ref = None;
        }
        if corruption == "unverified" {
            let existing = fixture
                .store
                .read_record(&scope(), invocation.inspection_ref.as_ref().unwrap())
                .await
                .unwrap();
            let mut observation: ModelRouteObservation =
                serde_json::from_value(existing.value().clone()).unwrap();
            observation.version_semantics = VersionSemantics::Unverified;
            let changed = ProtectedRecord::new(
                id("unverified-inspection"),
                1,
                serde_json::to_value(observation).unwrap(),
            );
            invocation.inspection_ref = Some(changed.reference().clone());
            records.push(changed);
        }
        let original = fixture
            .store
            .read_record(&scope(), invocation.prepared_step_ref.as_ref().unwrap())
            .await
            .unwrap();
        let mut root: PreparedStepRecord =
            serde_json::from_value(original.value().clone()).unwrap();
        let original_projection = fixture
            .store
            .read_record(&scope(), &root.context_projection)
            .await
            .unwrap();
        let mut projection: PreparedModelProjection =
            serde_json::from_value(original_projection.value().clone()).unwrap();
        projection.request.request_id = id("new-step");
        let mut physical = projection.request.clone();
        physical.request_id = invocation.attempt_id.clone();
        invocation.request_digest = physical.digest();
        let projection_record = ProtectedRecord::new(
            id("new-step-projection"),
            1,
            serde_json::to_value(projection).unwrap(),
        );
        if let Some(step) = records
            .iter()
            .find(|record| record.value()["schema_version"] == "wickle.model-step.v2")
        {
            root.step_input = step.reference().clone();
        }
        root.model_step_id = id("new-step");
        root.context_projection = projection_record.reference().clone();
        let root_record = ProtectedRecord::new(
            id("new-step-preparation"),
            1,
            serde_json::to_value(root).unwrap(),
        );
        invocation.prepared_step_ref = Some(root_record.reference().clone());
        let mut update = core::prepared(&saved, fixture.lease.clone(), 0);
        if let Some(step) = records
            .iter()
            .find(|record| record.value()["schema_version"] == "wickle.model-step.v2")
        {
            update
                .snapshot
                .model_step_inputs
                .push(step.reference().clone());
        }
        update.snapshot.model_step_id = Some(id("new-step"));
        update.snapshot.active_prepared_step = Some(root_record.reference().clone());
        update
            .snapshot
            .prepared_steps
            .push(root_record.reference().clone());
        records.extend([projection_record, root_record]);
        update.snapshot.model_ledger.push(invocation);
        update.records = records.clone();
        let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
        let mut value = serde_json::to_value(checkpoint).unwrap();
        value["runs"][0]["snapshot"] = serde_json::to_value(&update.snapshot).unwrap();
        for record in records {
            value["records"]
                .as_array_mut()
                .unwrap()
                .push(json!({"reference":record.reference(),"value":record.value()}));
        }
        value["records"]
            .as_array_mut()
            .unwrap()
            .sort_by_key(|record| {
                (
                    record["reference"]["record_id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    record["reference"]["revision"].as_u64().unwrap(),
                )
            });
        let restored = StateStoreCheckpoint::from_json(
            &value.to_string(),
            &scope(),
            &canonical_digest(&value),
        );
        let committed = fixture.store.commit(&scope(), &id("run"), update).await;
        assert_eq!(
            restored.is_ok(),
            corruption == "valid",
            "checkpoint inspection contract: {corruption}: {restored:?}"
        );
        assert_eq!(
            committed.is_ok(),
            corruption == "valid",
            "commit inspection contract: {corruption}: {committed:?}"
        );
    }
}

#[tokio::test]
async fn inspector_failures_are_bounded_and_do_not_spend_physical_model_calls() {
    for mode in [Inspection::Pending, Inspection::Panics] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        *fixture.inspector.mode.lock().unwrap() = mode;
        let exchange = fixture
            .exchange(0)
            .with_route_inspector(fixture.inspector.clone(), Duration::from_millis(30))
            .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            exchange.generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::ModelInspectionUnavailable);
        assert_eq!(fixture.call_counts(), (0, 0));
        assert_eq!(fixture.saved().await.usage.model_calls, 0);
        assert!(
            fixture
                .inspector
                .tokens
                .lock()
                .unwrap()
                .iter()
                .all(|token| token.is_cancelled())
        );
    }
}

#[tokio::test]
async fn agent_verification_and_compaction_use_the_same_budget_and_policy_dispatch_path() {
    let fixture = Fixture::new(
        vec![Reply::Complete, Reply::Complete, Reply::Complete],
        vec![],
    )
    .await;
    for (step, purpose) in [
        ("agent", ModelPurpose::Agent),
        ("verify", ModelPurpose::Verification),
        ("compact", ModelPurpose::Compaction),
    ] {
        let mut input = fixture.input(step);
        input.routing.purpose = purpose;
        completed(
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &input,
                    &fixture.projector,
                    &fixture.context(),
                    &fixture.budget().await,
                )
                .await
                .unwrap(),
        );
        assert!(
            fixture
                .policy
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(_, checked_purpose)| checked_purpose == &purpose)
        );
    }
    let saved = fixture.saved().await;
    assert_eq!(saved.usage.model_calls, 3);
    assert_eq!(saved.usage.recovery_attempts, 0);
    assert_eq!(
        saved
            .model_ledger
            .iter()
            .map(|entry| entry.purpose)
            .collect::<Vec<_>>(),
        [
            ModelPurpose::Agent,
            ModelPurpose::Verification,
            ModelPurpose::Compaction
        ]
    );
    assert_eq!(fixture.call_counts(), (3, 0));
    assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn exhausted_model_or_recovery_budget_prevents_fallback_dispatch_and_keeps_partial_failure() {
    for (model_calls, recoveries) in [(1, 4), (8, 0)] {
        let fixture = Fixture::with_limits(
            vec![Reply::Fail(ModelFailureKind::RateLimited)],
            vec![],
            model_calls,
            recoveries,
        )
        .await;
        let error = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::BudgetExceeded);
        assert_eq!(fixture.call_counts(), (1, 0));
        let saved = fixture.saved().await;
        assert_eq!(saved.usage.model_calls, 1);
        assert!(saved.usage.recovery_attempts <= recoveries);
        let record = fixture
            .store
            .read_record(
                &scope(),
                saved.model_ledger[0].response_ref.as_ref().unwrap(),
            )
            .await
            .unwrap();
        let stored: StoredModelResponse = serde_json::from_value(record.value().clone()).unwrap();
        let ModelExchangeOutcome::Failed { failure } = stored.outcome else {
            panic!("partial failure was lost")
        };
        assert_eq!(failure.kind, ModelFailureKind::RateLimited);
        assert_eq!(failure.partial_text(), "Partial response");
    }
}

#[tokio::test]
async fn caller_cancellation_reaches_a_hanging_inspector_even_with_a_distinct_run_token() {
    let fixture = Arc::new(Fixture::new(vec![Reply::Complete], vec![]).await);
    *fixture.inspector.mode.lock().unwrap() = Inspection::Pending;
    let context = fixture.context();
    let cancel = context.cancellation.clone();
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &context,
                    &fixture.budget().await,
                )
                .await
        })
    };
    fixture.inspector.entered.notified().await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::Cancelled);
    assert_eq!(fixture.call_counts(), (0, 0));
    assert!(fixture.inspector.tokens.lock().unwrap()[0].is_cancelled());
}

#[tokio::test]
async fn observed_drift_and_unavailability_use_only_the_explicit_finite_fallback_list() {
    for mode in [
        Inspection::DriftPrimary,
        Inspection::UnavailablePrimary,
        Inspection::UnavailableAll,
    ] {
        let fixture = Fixture::new(vec![], vec![Reply::Complete]).await;
        *fixture.inspector.mode.lock().unwrap() = mode;
        let result = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await;
        if matches!(mode, Inspection::UnavailableAll) {
            assert_eq!(result.unwrap_err().code, ErrorCode::ModelRoutesExhausted);
            assert_eq!(fixture.call_counts(), (0, 0));
        } else {
            completed(result.unwrap());
            assert_eq!(fixture.call_counts(), (0, 1));
        }
        assert_eq!(fixture.inspector.calls.lock().unwrap().len(), 2);
        assert_eq!(fixture.saved().await.usage.recovery_attempts, 1);
    }
}

#[tokio::test]
async fn unsettled_or_unknown_tool_effects_block_new_model_steps_and_fallback() {
    for state in [ToolCallState::Planned {}, unknown_tool_result()] {
        let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
        seed_tool(&fixture, state).await;
        let error = fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidTransition);
        assert_eq!(fixture.call_counts(), (0, 0));
        assert!(fixture.inspector.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn an_interrupted_physical_attempt_cannot_be_silently_reissued_on_resume() {
    let fixture = Arc::new(Fixture::new(vec![Reply::Pending, Reply::Complete], vec![]).await);
    let context = fixture.context();
    let cancel = context.cancellation.clone();
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            fixture
                .exchange(0)
                .generate_routed(
                    &fixture.router,
                    &fixture.input("step"),
                    &fixture.projector,
                    &context,
                    &fixture.budget().await,
                )
                .await
        })
    };
    fixture.first.entered.notified().await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err().code, ErrorCode::Cancelled);
    assert!(matches!(
        fixture.saved().await.model_ledger[0].state,
        ModelAttemptState::Unknown {}
    ));
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelAttemptUnresolved
    );
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &fixture.input("different-step"),
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::ModelAttemptUnresolved
    );
    assert_eq!(fixture.call_counts(), (1, 0));
}

#[tokio::test]
async fn agent_calls_cannot_substitute_the_profile_logical_binding() {
    let fixture = Fixture::new(vec![Reply::Complete], vec![]).await;
    let mut input = fixture.input("agent");
    input.routing.model_binding = id("another-slot");
    let error = fixture
        .exchange(0)
        .generate_routed(
            &fixture.router,
            &input,
            &fixture.projector,
            &fixture.context(),
            &fixture.budget().await,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::ModelRouteDenied);
    assert_eq!(fixture.call_counts(), (0, 0));
    assert_eq!(fixture.saved().await.usage.model_calls, 0);
}

#[tokio::test]
async fn prepared_fallback_survives_a_crash_before_dispatch_without_reprojection_or_double_charge()
{
    let mut fixture = Fixture::new(
        vec![Reply::Fail(ModelFailureKind::Transport)],
        vec![Reply::Complete],
    )
    .await;
    *fixture.inspector.mode.lock().unwrap() = Inspection::PendingFallback;
    let input = fixture.input("durable-fallback");
    {
        let exchange = fixture.exchange(0);
        let budget = fixture.budget().await;
        let context = fixture.context();
        let work = exchange.generate_routed(
            &fixture.router,
            &input,
            &fixture.projector,
            &context,
            &budget,
        );
        tokio::pin!(work);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut work => panic!("fallback should be paused before dispatch: {result:?}"),
                    _ = fixture.inspector.entered.notified() => {
                        if fixture.inspector.calls.lock().unwrap().last().is_some_and(|route| route.binding.id == id("fallback")) { break; }
                    }
                }
            }
        }).await.unwrap();
    }
    let before = fixture.saved().await;
    assert_eq!(fixture.call_counts(), (1, 0));
    assert_eq!(before.prepared_steps.len(), 2);
    assert_eq!(before.usage.recovery_attempts, 1);
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let restored = StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    fixture.store = Arc::new(MemoryStateStore::from_checkpoint(restored));
    *fixture.inspector.mode.lock().unwrap() = Inspection::Healthy;
    *fixture.projector.mode.lock().unwrap() = Projection::DifferentContent;
    fixture.second.panic_compiler.store(true, Ordering::SeqCst);
    completed(
        fixture
            .exchange(0)
            .generate_routed(
                &fixture.router,
                &input,
                &fixture.projector,
                &fixture.context(),
                &fixture.budget().await,
            )
            .await
            .unwrap(),
    );
    let after = fixture.saved().await;
    assert_eq!(after.prepared_steps, before.prepared_steps);
    assert_eq!(after.usage.recovery_attempts, 1);
    assert_eq!(fixture.call_counts(), (1, 1));
    assert_eq!(fixture.projector.calls.lock().unwrap().len(), 2);
    assert_eq!(fixture.second.compiler_lookups.load(Ordering::SeqCst), 1);
}
