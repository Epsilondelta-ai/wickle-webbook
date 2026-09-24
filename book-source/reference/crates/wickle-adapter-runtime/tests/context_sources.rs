//! Catalog and adapter-export sources share batch semantics while obeying scoped lifetimes.

#[path = "../../wickle/tests/support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/mod.rs"]
#[allow(dead_code)]
mod common;
#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[path = "support/context_sources.rs"]
#[allow(dead_code)]
mod support;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::*;
use wickle::*;

#[tokio::test]
async fn one_catalog_source_can_serve_both_triggers_without_duplicate_runtime_registration() {
    let fixture = Fixture::new(true, true);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    fixture.released(&handle).await;
    {
        let queries = fixture.catalog_source.queries.lock().unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(
            queries
                .iter()
                .map(|(request, _)| request.binding.trigger)
                .collect::<Vec<_>>(),
            vec![
                ContextTrigger::RunStart,
                ContextTrigger::BeforeModel,
                ContextTrigger::BeforeModel
            ]
        );
        assert_ne!(
            queries[0].0.context_request_id,
            queries[1].0.context_request_id
        );
        assert_ne!(
            queries[1].0.context_request_id,
            queries[2].0.context_request_id
        );
        assert!(
            queries
                .iter()
                .all(|(_, context)| context.binding_set_id.is_some())
        );
    }
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(saved.snapshot.context_batches.len(), 3);
    assert_eq!(saved.snapshot.source_states.len(), 2);
    let requests = fixture.model.requests.lock().unwrap();
    for request in &*requests {
        assert_eq!(request.messages.iter().flat_map(|message|&message.content).filter(|content|matches!(content,ModelContent::Json{value} if value["kind"]=="context_data"&&value["origin"]=="memory")).count(),2);
    }
    assert_eq!(
        fixture.factory.instances.lock().unwrap()[0]
            .context
            .selected_exports
            .len(),
        1
    );
}

#[tokio::test]
async fn adapter_source_batch_survives_waiting_while_fresh_instances_reauthorize_its_original_items()
 {
    let fixture = Fixture::new(false, false);
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    fixture.released(&original).await;
    let first = fixture.factory.instances.lock().unwrap()[0].clone();
    assert_eq!(first.closes.load(Ordering::SeqCst), 1);
    assert_eq!(first.source.queries.lock().unwrap().len(), 1);
    assert_eq!(first.tool.calls.load(Ordering::SeqCst), 0);
    let saved = fixture
        .base
        .store
        .load(&scope(), original.run_id())
        .await
        .unwrap();
    let wait = saved.snapshot.wait.clone().unwrap();
    let WaitTarget::Approval { target } = wait.target else {
        panic!("approval required")
    };
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("approve"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let mut reviewer = agent_support::context();
    reviewer.data.principal_ref = id("reviewer");
    let resumed = agent_support::completed(agent.resume(command.clone(), reviewer).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    fixture.released(&resumed).await;
    let second = fixture.factory.instances.lock().unwrap()[1].clone();
    assert_ne!(
        first.context.execution.binding_set_id,
        second.context.execution.binding_set_id
    );
    assert_eq!(second.context.execution.principal_ref, id("caller"));
    assert_eq!(second.source.queries.lock().unwrap().len(), 0);
    assert!(!second.source.uses.lock().unwrap().is_empty());
    assert!(
        second
            .source
            .uses
            .lock()
            .unwrap()
            .iter()
            .all(
                |(request, context)| request.batch_ref == saved.snapshot.context_batches[0]
                    && context.binding_set_id.as_ref()
                        == Some(&second.context.execution.binding_set_id)
                    && context.principal_ref == id("caller")
                    && request.items[0].item_id == id("local-item")
            )
    );
    assert_eq!(second.tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .base
            .store
            .load(&scope(), resumed.run_id())
            .await
            .unwrap()
            .snapshot
            .context_batches,
        saved.snapshot.context_batches
    );
    let replay = agent_support::completed(
        agent
            .resume(command, agent_support::context())
            .await
            .unwrap(),
    );
    fixture.outcome(&replay).await;
    assert_eq!(fixture.factory.instances.lock().unwrap().len(), 2);
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_fresh_source_instance_can_deny_cached_data_without_fetching_or_sending_it_again() {
    let fixture = Fixture::new(false, false);
    fixture.policy.approval.store(true, Ordering::SeqCst);
    let agent = fixture.agent();
    let original = fixture.start(&agent).await;
    fixture.outcome(&original).await;
    fixture.released(&original).await;
    let saved = fixture
        .base
        .store
        .load(&scope(), original.run_id())
        .await
        .unwrap();
    let wait = saved.snapshot.wait.clone().unwrap();
    let WaitTarget::Approval { target } = wait.target else {
        unreachable!()
    };
    fixture.factory.revoke_new.store(true, Ordering::SeqCst);
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: saved.snapshot.revision,
        command_id: id("approve"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let resumed = agent_support::completed(
        agent
            .resume(command, agent_support::context())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Failed
    );
    fixture.released(&resumed).await;
    let second = fixture.factory.instances.lock().unwrap()[1].clone();
    assert!(second.source.queries.lock().unwrap().is_empty());
    assert!(!second.source.uses.lock().unwrap().is_empty());
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observers_only_keeps_source_metadata_but_cannot_activate_queries_or_cached_acl_callbacks()
{
    let fixture = Fixture::new(false, false);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    fixture.outcome(&handle).await;
    fixture.released(&handle).await;
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    let assembly_ref = saved.snapshot.assembly_ref.as_ref().unwrap();
    let record = fixture
        .base
        .store
        .read_record(&scope(), assembly_ref)
        .await
        .unwrap();
    let assembly = ResolvedAssembly::restore(
        &record.value().to_string(),
        &saved.snapshot.profile,
        &common::inputs(),
        &assembly_ref.digest,
    )
    .unwrap();
    let context = ComponentBindContext {
        scope: scope(),
        run_id: handle.run_id().clone(),
        session_id: id("session"),
        binding_set_id: id("observer-only"),
        principal_ref: id("caller"),
        capability_grant_ref: id("grant"),
        lease: None,
        purpose: ComponentBindPurpose::ObserversOnly,
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    };
    let bound = fixture.runtime().bind(&assembly, &context).await.unwrap();
    let source = ContextSourceRuntime::new(
        fixture.base.store.clone(),
        Arc::new(PolicyGate::new(fixture.policy.clone(), Duration::from_secs(5)).unwrap()),
        fixture.base.clock.clone(),
        fixture.base.ids.clone(),
        bound.sources().clone(),
        Arc::new(Estimate),
    )
    .unwrap()
    .with_binding_set_id(context.binding_set_id.clone());
    assert!(
        source
            .authorize_use(
                handle.run_id(),
                &saved.snapshot.context_batches,
                None,
                &agent_support::context(),
                tokio::time::Instant::now() + Duration::from_secs(5)
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.factory.instances.lock().unwrap().len(), 1);
    assert!(
        bound
            .release(&ComponentReleaseContext {
                scope: scope(),
                run_id: handle.run_id().clone(),
                binding_set_id: context.binding_set_id,
                cancellation: Default::default(),
                deadline: tokio::time::Instant::now() + Duration::from_secs(5)
            })
            .await
            .unwrap()
            .failures
            .is_empty()
    );
}

#[tokio::test]
async fn an_export_used_by_two_triggers_opens_once_and_uses_two_independent_active_slots() {
    let fixture = Fixture::new(false, true);
    let agent = fixture.agent();
    let handle = fixture.start(&agent).await;
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    fixture.released(&handle).await;
    let instance = fixture.factory.instances.lock().unwrap()[0].clone();
    assert_eq!(
        instance
            .context
            .selected_exports
            .iter()
            .filter(|export| export.export_id == id("recall"))
            .count(),
        1
    );
    assert_eq!(instance.source.queries.lock().unwrap().len(), 3);
    let saved = fixture
        .base
        .store
        .load(&scope(), handle.run_id())
        .await
        .unwrap();
    assert_eq!(saved.snapshot.source_states.len(), 2);
    assert_eq!(saved.snapshot.context_batches.len(), 3);
    assert_ne!(
        saved.snapshot.source_states[0].batch_ref,
        saved.snapshot.source_states[1].batch_ref
    );
    assert_eq!(instance.closes.load(Ordering::SeqCst), 1);
    assert_eq!(instance.tool.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn repeating_the_same_source_trigger_is_rejected_even_with_different_limits() {
    for catalog in [false, true] {
        let fixture = Fixture::new(catalog, true);
        let mut selected = fixture.profile.clone();
        let bindings = selected.context_sources.as_mut().unwrap();
        bindings[1].trigger = ContextTrigger::RunStart;
        bindings[1].max_items = 1.try_into().unwrap();
        let error = ProfileValidator::new(&common::RegistryResolver(&fixture.registry))
            .validate(&selected, &scope())
            .await
            .unwrap_err();
        assert_eq!(error.path, "context_sources.duplicate");
        assert!(fixture.factory.instances.lock().unwrap().is_empty());
        assert!(fixture.catalog_source.queries.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn interrupted_model_recovery_reuses_saved_batches_with_new_scoped_instances_and_current_access()
 {
    for revoked in [false, true] {
        let mut fixture = Fixture::new(false, true);
        fixture.profile.limits.max_recovery_attempts = 2;
        let mut bindings = fixture.bindings();
        bindings.state = Arc::new(agent_support::FinalCommitStore::new(
            fixture.base.store.clone(),
            agent_support::FinalCommitMode::RejectCandidate,
        ));

        let original_agent = create_agent(fixture.profile.clone(), bindings).unwrap();

        let original = fixture.start(&original_agent).await;
        assert_eq!(
            original
                .outcome(&agent_support::context())
                .await
                .unwrap_err()
                .code,
            ErrorCode::PersistenceUnavailable
        );

        fixture.released(&original).await;

        let saved = fixture
            .base
            .store
            .load(&scope(), original.run_id())
            .await
            .unwrap();
        assert_eq!(saved.snapshot.status, RunStatus::Running);
        assert!(!saved.snapshot.context_batches.is_empty());
        let model_calls = fixture.model.calls.load(Ordering::SeqCst);
        let first = fixture.factory.instances.lock().unwrap()[0].clone();
        assert_eq!(first.closes.load(Ordering::SeqCst), 1);
        let queries = first.source.queries.lock().unwrap().len();
        fixture.factory.revoke_new.store(revoked, Ordering::SeqCst);
        let checkpoint = saved
            .snapshot
            .recovery_record(id("interrupted-context"))
            .unwrap();
        let command = ResumeCommand {
            run_id: original.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id("recover-context"),
            action: ResumeAction::Recover {
                recovery_ref: checkpoint.reference().clone(),
            },
        };

        let recovered = agent_support::completed(
            fixture
                .agent()
                .resume(command, agent_support::context())
                .await
                .unwrap(),
        );

        let outcome = fixture.outcome(&recovered).await;
        assert_eq!(
            outcome.result.status(),
            if revoked {
                RunStatus::Failed
            } else {
                RunStatus::Succeeded
            }
        );
        fixture.released(&recovered).await;
        assert_eq!(fixture.model.calls.load(Ordering::SeqCst), model_calls);
        {
            let instances = fixture.factory.instances.lock().unwrap();
            assert_eq!(instances.len(), 2);
            assert_eq!(instances[1].closes.load(Ordering::SeqCst), 1);
            assert_ne!(
                instances[0].context.execution.binding_set_id,
                instances[1].context.execution.binding_set_id
            );
            assert!(instances[1].source.queries.lock().unwrap().is_empty());
            assert_eq!(instances[0].source.queries.lock().unwrap().len(), queries);
        }
        assert_eq!(
            fixture
                .base
                .store
                .load(&scope(), recovered.run_id())
                .await
                .unwrap()
                .snapshot
                .context_batches,
            saved.snapshot.context_batches
        );
    }
}
