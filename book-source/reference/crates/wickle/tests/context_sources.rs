//! Automatic context collection persists source batches and rechecks live access before every use.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_hooks.rs"]
#[allow(dead_code)]
mod hooks_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod resume_support;
#[path = "support/context_sources.rs"]
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use support::*;
use wickle::*;

#[tokio::test]
async fn identical_native_item_ids_from_two_sources_keep_profile_order_and_separate_namespaces() {
    let mut fixture = Fixture::new();
    let beta = fixture.add("beta", ContextTrigger::RunStart, true, vec![Reply::Ready]);
    let alpha = fixture.add("alpha", ContextTrigger::RunStart, true, vec![Reply::Ready]);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(
        *fixture.order.lock().unwrap(),
        vec![id("beta"), id("alpha")]
    );
    assert_eq!(beta.calls.load(Ordering::SeqCst), 1);
    assert_eq!(alpha.calls.load(Ordering::SeqCst), 1);
    let requests = fixture.model.requests.lock().unwrap();
    for request in &*requests {
        let items = projected_items(request);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["source_ref"]["id"], json!("beta"));
        assert_eq!(items[1]["source_ref"]["id"], json!("alpha"));
        assert_ne!(items[0]["item_id"], items[1]["item_id"]);
        assert!(items.iter().all(|item| item["origin"] == "retrieval"));
    }
    assert_eq!(projected_items(&requests[0]), projected_items(&requests[1]));
}

#[tokio::test]
async fn required_empty_is_valid_but_required_unavailable_stops_before_the_model() {
    for (required, reply, success) in [
        (true, Reply::Empty, true),
        (false, Reply::Unavailable, true),
        (true, Reply::Unavailable, false),
    ] {
        let mut fixture = Fixture::new();
        let source = fixture.add("records", ContextTrigger::RunStart, required, vec![reply]);
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        let outcome = fixture.outcome(&handle).await;
        assert_eq!(
            outcome.result.status(),
            if success {
                RunStatus::Succeeded
            } else {
                RunStatus::Failed
            }
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        if success {
            assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 2);
            assert!(
                fixture
                    .model
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|request| projected_items(request).is_empty())
            );
        } else {
            assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
        }
        let saved = fixture.saved(&handle).await;
        assert_eq!(saved.snapshot.context_batches.len(), 1);
        assert_eq!(saved.snapshot.source_states.len(), 1);
    }
}

#[tokio::test]
async fn malformed_claims_and_limits_fail_closed_even_when_the_source_is_optional() {
    for reply in [
        Reply::WrongScope,
        Reply::WrongOrigin,
        Reply::WrongLifetime,
        Reply::WrongDigest,
        Reply::TooMany,
        Reply::TooLarge,
        Reply::ReadyEmpty,
        Reply::Error,
    ] {
        let mut fixture = Fixture::new();
        let source = fixture.add("records", ContextTrigger::RunStart, false, vec![reply]);
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Failed
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .base
                .base
                .tools
                .iter()
                .all(|tool| tool.calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test]
async fn host_token_estimate_is_enforced_independently_of_byte_and_item_limits() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::RunStart,
        false,
        vec![Reply::Ready],
    );
    fixture.estimator.tokens.store(101, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(fixture.estimator.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_empty_or_unavailable_batch_replaces_the_active_source_slot_without_reusing_old_items()
 {
    for second in [Reply::Empty, Reply::Unavailable] {
        let mut fixture = Fixture::new();
        let source = fixture.add(
            "records",
            ContextTrigger::BeforeModel,
            false,
            vec![Reply::Ready, second],
        );
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Succeeded
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
        let saved = fixture.saved(&handle).await;
        assert_eq!(saved.snapshot.context_batches.len(), 2);
        assert_eq!(saved.snapshot.source_states.len(), 1);
        assert_eq!(
            saved.snapshot.source_states[0].batch_ref,
            saved.snapshot.context_batches[1]
        );
        let requests = fixture.model.requests.lock().unwrap();
        assert_eq!(projected_items(&requests[0]).len(), 1);
        assert!(projected_items(&requests[1]).is_empty());
    }
}

#[tokio::test]
async fn transport_retry_reuses_the_batch_but_checks_access_for_each_physical_attempt() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready],
    );
    fixture.model.fail_first.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 3);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert!(source.uses.load(Ordering::SeqCst) >= 3);
    let requests = fixture.model.requests.lock().unwrap();
    assert_eq!(projected_items(&requests[0]), projected_items(&requests[1]));
}

#[tokio::test]
async fn revocation_after_a_transport_failure_blocks_the_next_model_attempt_and_hook_derived_data()
{
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready],
    );
    let hook = fixture.enable_copy_hook();
    fixture.model.fail_first.store(true, Ordering::SeqCst);
    fixture
        .model
        .revoke_on_failure
        .store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    let outcome = handle.outcome(&context()).await.unwrap_or_else(|error| {
        panic!(
            "{error:?}; provide={}, use={}, model={}, hook={}",
            source.calls.load(Ordering::SeqCst),
            source.uses.load(Ordering::SeqCst),
            fixture.model.physical_calls.load(Ordering::SeqCst),
            hook.calls.load(Ordering::SeqCst)
        )
    });
    assert_eq!(completed(outcome).result.status(), RunStatus::Failed);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert!(source.uses.load(Ordering::SeqCst) >= 2);
    let inputs = hook.seen.lock().unwrap();
    assert!(
        inputs[0]
            .iter()
            .any(|item| item.source_ref == reference("records"))
    );
    let requests = fixture.model.requests.lock().unwrap();
    let projected = projected_items(&requests[0]);
    let original = projected
        .iter()
        .find(|item| item["origin"] == "retrieval")
        .unwrap();
    let copied = projected
        .iter()
        .find(|item| item["origin"] == "hook")
        .unwrap();
    assert_eq!(original["content"], copied["content"]);
}

#[tokio::test]
async fn revoked_cached_access_prevents_local_hooks_from_receiving_the_source_material() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        false,
        vec![Reply::Ready],
    );
    let hook = fixture.enable_copy_hook();
    fixture.revoked.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn approval_resume_reuses_run_start_context_while_a_new_logical_step_queries_step_context() {
    let mut fixture = Fixture::new();
    let run = fixture.add(
        "run-records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready],
    );
    let step = fixture.add(
        "step-records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::Ready, Reply::Ready],
    );
    fixture.base.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(step.calls.load(Ordering::SeqCst), 1);
    let saved = fixture.saved(&original).await;
    let command = fixture.base.base.approve(&original, "approve").await;
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(run.calls.load(Ordering::SeqCst), 1);
    assert_eq!(step.calls.load(Ordering::SeqCst), 2);
    assert!(run.uses.load(Ordering::SeqCst) >= 2);
    assert!(
        fixture
            .saved(&resumed)
            .await
            .snapshot
            .context_batches
            .starts_with(&saved.snapshot.context_batches)
    );
}

#[tokio::test(start_paused = true)]
async fn optional_timeout_is_persisted_without_late_data_and_explicit_cancellation_is_not_optional()
{
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        false,
        vec![Reply::Paused],
    );
    fixture.bindings[0].timeout_ms = 10.try_into().unwrap();
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = fixture.saved(&handle).await;
    source.release.add_permits(1);
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(fixture.saved(&handle).await.snapshot, saved.snapshot);
    assert!(
        fixture
            .model
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| projected_items(request).is_empty())
    );

    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        false,
        vec![Reply::Paused],
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    gate(&source.entered).await;
    completed(
        handle
            .cancel(id("cancel-source"), &context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Cancelled
    );
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_policy_denial_prevents_provide_or_acl_callbacks_even_for_optional_sources() {
    for deny_provide in [false, true] {
        let mut fixture = Fixture::new();
        let source = fixture.add(
            "records",
            ContextTrigger::RunStart,
            false,
            vec![Reply::Ready],
        );
        fixture
            .policy
            .deny_provide
            .store(deny_provide, Ordering::SeqCst);
        fixture
            .policy
            .deny_use
            .store(!deny_provide, Ordering::SeqCst);
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Failed
        );
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            usize::from(!deny_provide)
        );
        assert_eq!(source.uses.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn failed_batch_storage_stops_model_use_and_recollection_distinguishes_unsaved_from_ack_lost()
{
    for failure in [1, 2] {
        let mut fixture = Fixture::new();
        let source = fixture.add(
            "records",
            ContextTrigger::RunStart,
            true,
            vec![Reply::Ready, Reply::Ready],
        );
        fixture.store.failure.store(failure, Ordering::SeqCst);
        let bindings = fixture.agent_bindings();
        let runtime = bindings.context_sources.as_ref().unwrap().clone();
        let agent = create_agent(fixture.profile(), bindings).unwrap();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            handle.outcome(&context()).await.unwrap_err().code,
            ErrorCode::PersistenceUnavailable
        );
        let before = fixture.saved(&handle).await;
        assert_eq!(
            before.snapshot.context_batches.len(),
            usize::from(failure == 2)
        );
        assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
        let lease = fixture
            .store
            .acquire_lease(
                &scope(),
                handle.run_id(),
                &id("source-replay"),
                fixture.base.base.base.clock.now().unwrap().utc_ms,
                30_000,
            )
            .await
            .unwrap();
        let budget = RunBudget::attach(
            fixture.store.clone(),
            fixture.base.base.base.clock.clone(),
            fixture.base.base.base.ids.clone(),
            scope(),
            handle.run_id().clone(),
            lease,
            Default::default(),
        )
        .await
        .unwrap();
        let batches = runtime
            .collect(ContextTrigger::RunStart, None, &context(), &budget)
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            if failure == 1 { 2 } else { 1 }
        );
        assert_eq!(
            batches[0].items()[0].content,
            vec![InputContent::Json {
                value: json!({"source":"records","generation":if failure==1{1}else{0}})
            }]
        );
        let saved = fixture.saved(&handle).await;
        assert_eq!(saved.snapshot.context_batches, vec![batches[0].reference()]);
        assert_eq!(
            saved.snapshot.source_states[0].batch_ref,
            batches[0].reference()
        );
    }
}

#[tokio::test]
async fn provider_usage_is_preserved_separately_from_the_host_estimate_and_local_acl_item_ids() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready],
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = fixture.saved(&handle).await;
    let plan_ref = saved.snapshot.source_plan_ref.as_ref().unwrap();
    let plan_record = fixture.store.read_record(&scope(), plan_ref).await.unwrap();
    let plan =
        ContextSourcePlan::restore(&plan_record.value().to_string(), &scope(), &plan_ref.digest)
            .unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &saved.snapshot.context_batches[0])
        .await
        .unwrap();
    let batch = ContextBatch::restore(&record, &plan, &scope(), handle.run_id()).unwrap();
    assert_eq!(batch.estimated_tokens(), 16);
    assert_eq!(
        batch.estimator_version(),
        &reference("fixture-context-estimate")
    );
    assert!(matches!(
        batch.result(),
        ContextResult::Ready {
            reported_usage: Some(ContextSourceUsage {
                tokens: Some(1_000_000),
                requests: Some(1)
            }),
            ..
        }
    ));
    assert_eq!(batch.result().items()[0].item_id, id("shared"));
    assert_ne!(batch.items()[0].item_id, id("shared"));
    let uses = source.use_requests.lock().unwrap();
    assert!(
        uses.iter()
            .all(|request| request.batch_ref == batch.reference()
                && request.items == batch.result().items())
    );
    assert!(uses.iter().any(|request| request.route.is_none()));
    assert!(uses.iter().any(|request| request.route.is_some()));
}

#[tokio::test]
async fn a_new_run_queries_again_and_does_not_inherit_the_previous_run_source_items() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready, Reply::Ready],
    );
    let agent = fixture.agent();
    let first = fixture.start(&agent).await;
    fixture.outcome(&first).await;
    let mut caller = context();
    caller.data.system_inputs = Some(SystemInputs::new(object(
        json!({"workspace_id":resume_support::WORKSPACE}),
    )));
    let second = completed(
        agent
            .start(request("second-request"), caller)
            .await
            .unwrap(),
    );
    fixture.outcome(&second).await;
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    let requests = fixture.model.requests.lock().unwrap();
    let prior = projected_items(&requests[0]);
    let current = projected_items(requests.last().unwrap());
    assert_eq!(current.len(), 1);
    assert_ne!(prior[0]["item_id"], current[0]["item_id"]);
    assert_eq!(
        current[0]["content"][0]["value"],
        json!({"source":"records","generation":1})
    );
}

#[tokio::test]
async fn source_access_errors_never_become_model_unavailable_or_version_drift_fallbacks() {
    for (code, cause) in [
        (ErrorCode::ModelUnavailable, ModelFailureKind::Unavailable),
        (ErrorCode::ModelVersionDrift, ModelFailureKind::VersionDrift),
    ] {
        for check_at in [3, 4] {
            let mut fixture = Fixture::new();
            let source = fixture.add(
                "records",
                ContextTrigger::RunStart,
                true,
                vec![Reply::Ready],
            );
            // First check is local preparation, second is the initial projection.
            // The following checks are physical pre-reservation and final dispatch.
            *source.use_failure.lock().unwrap() = Some((check_at, code));
            let mut bindings = fixture.agent_bindings();
            let router =
                std::sync::Arc::new(FallbackRouter::new(bindings.router.snapshot(), cause));
            bindings.router = router.clone();
            let agent = create_agent(fixture.profile(), bindings).unwrap();
            let handle = fixture.start(&agent).await;
            let outcome = fixture.outcome(&handle).await;
            assert_eq!(
                router.fallbacks.load(Ordering::SeqCst),
                0,
                "source error selected a fallback; physical calls={}",
                fixture.model.physical_calls.load(Ordering::SeqCst)
            );
            let OutcomeResult::Failed { failure } = outcome.result else {
                panic!("source error did not stop the run");
            };
            assert_eq!(failure.code, id("invalid_context"));
            assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
            assert_eq!(source.calls.load(Ordering::SeqCst), 1);
            let saved = fixture.saved(&handle).await;
            assert_eq!(saved.snapshot.context_batches.len(), 1);
            assert_eq!(
                saved.snapshot.source_states[0].batch_ref,
                saved.snapshot.context_batches[0]
            );
            assert_eq!(saved.snapshot.usage.recovery_attempts, 0);
            assert_eq!(saved.snapshot.usage.model_calls, u64::from(check_at == 4));
        }
    }
}

async fn saved_batches(fixture: &Fixture, handle: &RunHandle) -> Vec<ContextBatch> {
    let saved = fixture.saved(handle).await;
    let plan_ref = saved.snapshot.source_plan_ref.as_ref().unwrap();
    let record = fixture.store.read_record(&scope(), plan_ref).await.unwrap();
    let plan = ContextSourcePlan::restore(&record.value().to_string(), &scope(), &plan_ref.digest)
        .unwrap();
    let mut batches = vec![];
    for reference in &saved.snapshot.context_batches {
        let record = fixture
            .store
            .read_record(&scope(), reference)
            .await
            .unwrap();
        batches.push(ContextBatch::restore(&record, &plan, &scope(), handle.run_id()).unwrap());
    }
    batches
}

fn data_fragment(batch: &ContextBatch) -> &ContextFragment {
    batch
        .fragments()
        .iter()
        .find(|fragment| matches!(fragment.value, FragmentValue::Item { .. }))
        .unwrap()
}

#[tokio::test]
async fn new_observation_advances_core_revision_without_inventing_external_versions() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::ReadySame, Reply::ReadySame],
    );
    fixture.model.fail_first.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let batches = saved_batches(&fixture, &handle).await;
    assert_eq!(batches.len(), 2);
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 3);
    let first = data_fragment(&batches[0]);
    let second = data_fragment(&batches[1]);
    assert_eq!(first.identity, second.identity);
    assert_eq!(first.core_revision.get(), 1);
    assert_eq!(second.core_revision.get(), 2);
    assert_eq!(first.content_digest, second.content_digest);
    assert_eq!(first.source_revision, None);
    assert_eq!(second.source_revision, None);
    assert_ne!(first.batch_id, second.batch_id);
    assert!(batches[1].fragments().iter().any(|fragment| matches!(
        fragment.value,
        FragmentValue::Selection { .. }
    ) && fragment.source_revision
        == Some(id("revision-1"))));
}

#[tokio::test]
async fn document_revision_is_distinct_from_index_revision_and_reaches_authorization() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::ReadyVersion],
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let batches = saved_batches(&fixture, &handle).await;
    assert_eq!(
        data_fragment(&batches[0]).source_revision,
        Some(id("document-r1"))
    );
    let checks = source.use_requests.lock().unwrap();
    assert!(checks.iter().any(|request| request.derived));
    assert!(
        checks
            .iter()
            .all(|request| request.item_revisions.get(&id("shared")) == Some(&id("document-r1")))
    );
}

#[tokio::test]
async fn empty_selection_tombstones_prevent_falling_back_to_older_content() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::Ready, Reply::Empty],
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let batches = saved_batches(&fixture, &handle).await;
    let fragments: Vec<_> = batches
        .iter()
        .flat_map(|batch| batch.fragments().iter().cloned())
        .collect();
    assert!(
        select_context_fragments(&fragments, &scope())
            .unwrap()
            .is_empty()
    );
    let tombstone = batches[1]
        .fragments()
        .iter()
        .find(|fragment| fragment.identity == data_fragment(&batches[0]).identity)
        .unwrap();
    assert_eq!(tombstone.core_revision.get(), 2);
    assert!(
        matches!(&tombstone.value, FragmentValue::Tombstone { reason } if reason == &id("empty"))
    );
    let mut other_scope = scope();
    other_scope.tenant_id = id("other-tenant");
    assert_eq!(
        select_context_fragments(&fragments, &other_scope)
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
}

#[tokio::test]
async fn explicit_deletion_blocks_derived_history_before_another_model_call() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::Ready, Reply::Deleted],
    );
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 1);
    let batches = saved_batches(&fixture, &handle).await;
    assert!(batches[1].fragments().iter().any(|fragment| matches!(&fragment.value, FragmentValue::Tombstone { reason } if reason == &id("deleted"))));
}

#[tokio::test]
async fn deletion_in_a_new_run_blocks_previous_run_derived_history() {
    let mut fixture = Fixture::new();
    let source = fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready, Reply::Deleted, Reply::Empty],
    );
    let agent = fixture.agent();
    let first = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&first).await.result.status(),
        RunStatus::Succeeded
    );
    let calls = fixture.model.physical_calls.load(Ordering::SeqCst);
    let mut caller = context();
    caller.data.system_inputs = Some(SystemInputs::new(object(
        json!({"workspace_id":resume_support::WORKSPACE}),
    )));
    let second = completed(
        agent
            .start(request("second-request"), caller)
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&second).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), calls);
    let third = completed(
        agent
            .start(request("third-request"), context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&third).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), calls);
    assert_eq!(source.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn deletion_from_another_trigger_blocks_still_active_run_start_data() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::RunStart,
        true,
        vec![Reply::Ready, Reply::Deleted],
    );
    let mut step_binding = fixture.bindings[0].clone();
    step_binding.trigger = ContextTrigger::BeforeModel;
    fixture.bindings.push(step_binding);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Failed
    );
    assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
    assert_eq!(saved_batches(&fixture, &handle).await.len(), 2);
}

#[tokio::test]
async fn reconciled_tool_observation_retains_the_original_model_step_sources() {
    let mut fixture = Fixture::new();
    fixture.add(
        "records",
        ContextTrigger::BeforeModel,
        true,
        vec![Reply::Ready, Reply::Ready],
    );
    let bindings = fixture.agent_bindings();
    let runtime = bindings.context_sources.clone().unwrap();
    let agent = create_agent(fixture.profile(), bindings).unwrap();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    let saved = fixture.saved(&handle).await;
    let mut correction = saved
        .messages
        .iter()
        .find(|message| message.origin == MessageOrigin::Tool)
        .unwrap()
        .clone();
    let ContentBlock::ToolResult { result } = correction.content[0].clone() else {
        panic!("tool observation")
    };
    correction.content = vec![ContentBlock::ToolResultCorrection {
        previous_message_id: correction.message_id.clone(),
        previous_result_digest: canonical_digest(&serde_json::to_value(&result).unwrap()),
        result,
    }];
    let lineage = runtime
        .lineage_for_messages(
            &saved.snapshot.request.session_id,
            &[correction],
            &context(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(
        lineage,
        vec![ContextLineage {
            run_id: handle.run_id().clone(),
            batch_ref: saved.snapshot.context_batches[0].clone()
        }]
    );
}

#[tokio::test]
async fn preparation_commit_failure_or_lost_ack_never_dispatches_or_charges_a_model() {
    for failure in [1, 2] {
        let mut fixture = Fixture::new();
        let source = fixture.add(
            "records",
            ContextTrigger::RunStart,
            true,
            vec![Reply::Ready],
        );
        fixture
            .store
            .preparation_failure
            .store(failure, Ordering::SeqCst);
        let agent = fixture.agent();
        let handle = fixture.start(&agent).await;
        assert_eq!(
            handle.outcome(&context()).await.unwrap_err().code,
            ErrorCode::PersistenceUnavailable
        );
        let saved = fixture.saved(&handle).await;
        assert_eq!(
            saved.snapshot.prepared_steps.len(),
            usize::from(failure == 2)
        );
        assert_eq!(saved.snapshot.model_step_inputs.len(), 1);
        assert_eq!(saved.snapshot.usage.model_calls, 0);
        assert!(saved.snapshot.model_ledger.is_empty());
        assert!(saved.snapshot.tool_ledger.is_empty());
        assert_eq!(fixture.model.physical_calls.load(Ordering::SeqCst), 0);
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        let image =
            serde_json::to_value(fixture.store.inner.export_checkpoint(&scope()).unwrap()).unwrap();
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .unwrap();
    }
}
