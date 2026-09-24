//! Adapter resources open after durable admission and close without repeating business effects.

#[path = "../../wickle/tests/support/mod.rs"]
#[allow(dead_code)]
mod core_fixture;
#[allow(dead_code)]
mod support;
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::*;
use wickle::*;
use wickle_adapter_runtime::*;

#[tokio::test]
async fn an_exact_tool_subset_opens_only_after_admission_and_lease_then_releases_once() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let assembly = fixture.resolve(&registry, &profile()).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let (admitted, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let bound = runtime.bind(&admitted, &context).await.unwrap();
    assert_eq!(
        serde_json::to_value(assembly).unwrap(),
        serde_json::to_value(admitted).unwrap()
    );
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.factories[0].observed.lock().unwrap()[0].selected_exports,
        vec![ExportRef {
            adapter_binding: id("records"),
            export_id: id("search"),
            alias: Some(id("search_records"))
        }]
    );
    assert!(bound.tools().get(&id("search_records")).is_some());
    assert!(bound.tools().get(&id("recall")).is_none());
    assert!(bound.tools().get(&id("record")).is_none());
    assert_eq!(bound.scope(), &scope());
    assert_eq!(bound.binding_set_id(), &context.binding_set_id);
    let report = bound.release(&release_context(&context)).await.unwrap();
    assert!(report.failures.is_empty());
    assert_eq!(
        bound.release(&release_context(&context)).await.unwrap(),
        report
    );
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:records", "close:records"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .close_calls
            .load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn missing_admission_wrong_scope_or_lost_lease_prevents_factory_entry() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut absent = context.clone();
    absent.run_id = id("not-admitted");
    assert!(runtime.bind(&assembly, &absent).await.is_err());
    let mut foreign = context.clone();
    foreign.scope.tenant_id = id("foreign");
    assert!(runtime.bind(&assembly, &foreign).await.is_err());
    let mut lease_missing = context.clone();
    lease_missing.lease = None;
    assert!(runtime.bind(&assembly, &lease_missing).await.is_err());
    fixture
        .store
        .release_lease(
            &scope(),
            &context.run_id,
            context.lease.as_ref().unwrap(),
            fixture.clock.now().unwrap().utc_ms,
        )
        .await
        .unwrap();
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_later_open_failure_closes_earlier_instances_in_reverse_without_publishing_tools() {
    for behavior in [1, 5, 6] {
        let mut fixture = Fixture::new();
        fixture.add_adapter("second");
        fixture.add_adapter("third");
        fixture.factories[2]
            .behavior
            .store(behavior, Ordering::SeqCst);
        let selected = multi_profile(&["adapter", "second", "third"]);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture.runtime(registry.clone());
        let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
        assert!(runtime.bind(&assembly, &context).await.is_err());
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec![
                "open:binding-0",
                "open:binding-1",
                "open:binding-2",
                "close:binding-1",
                "close:binding-0"
            ]
        );
        for factory in &fixture.factories[..2] {
            let instances = factory.instances.lock().unwrap();
            assert_eq!(instances[0].executor.calls.load(Ordering::SeqCst), 0);
            assert_eq!(instances[0].close_calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn wrong_runtime_descriptor_or_unrequested_exports_fail_attestation_and_close_the_instance() {
    for behavior in [2, 3] {
        let fixture = Fixture::new();
        fixture.factories[0]
            .behavior
            .store(behavior, Ordering::SeqCst);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture.runtime(registry.clone());
        let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
        assert!(runtime.bind(&assembly, &context).await.is_err());
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec!["open:records", "close:records"]
        );
        assert_eq!(
            fixture.factories[0].instances.lock().unwrap()[0]
                .executor
                .calls
                .load(Ordering::SeqCst),
            0
        );
    }
}

#[tokio::test(start_paused = true)]
async fn close_error_panic_and_timeout_do_not_skip_remaining_reverse_cleanup_or_change_the_run() {
    for behavior in [1, 2, 3] {
        let mut fixture = Fixture::new();
        fixture.add_adapter("second");
        fixture.factories[1]
            .close_behavior
            .store(behavior, Ordering::SeqCst);
        let selected = multi_profile(&["adapter", "second"]);
        let registry = Arc::new(fixture.registry().unwrap());
        let runtime = fixture
            .runtime(registry.clone())
            .with_settings(AdapterRuntimeSettings {
                open_timeout_ms: 30_000,
                close_timeout_ms: 10,
            })
            .unwrap();
        let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
        let bound = runtime.bind(&assembly, &context).await.unwrap();
        let saved = fixture.store.load(&scope(), &context.run_id).await.unwrap();
        let report = bound.release(&release_context(&context)).await.unwrap();
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].adapter_binding, id("binding-1"));
        assert_eq!(
            *fixture.events.lock().unwrap(),
            vec![
                "open:binding-0",
                "open:binding-1",
                "close:binding-1",
                "close:binding-0"
            ]
        );
        assert_eq!(
            fixture
                .store
                .load(&scope(), &context.run_id)
                .await
                .unwrap()
                .snapshot,
            saved.snapshot
        );
        assert_eq!(
            bound.release(&release_context(&context)).await.unwrap(),
            report
        );
        assert_eq!(
            fixture.factories[0].instances.lock().unwrap()[0]
                .close_calls
                .load(Ordering::SeqCst),
            1
        );
    }
}

#[tokio::test]
async fn surviving_tool_handles_cannot_execute_after_release_or_with_another_segment_or_scope() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    let tool = bound
        .tools()
        .get(&id("search_records"))
        .unwrap()
        .executor
        .clone();
    let arguments =
        object(json!({"query":"report","workspace_id":"11111111-1111-4111-8111-111111111111"}));
    let execution = execution_context(&context);
    assert!(tool.execute(&arguments, &execution).await.is_ok());
    let mut other = execution.clone();
    other.binding_set_id = Some(id("other-segment"));
    assert!(tool.execute(&arguments, &other).await.is_err());
    other = execution.clone();
    other.run_id = id("other-run");
    assert!(tool.execute(&arguments, &other).await.is_err());
    other = execution.clone();
    other.scope.workspace_id = id("other-workspace");
    assert!(tool.execute(&arguments, &other).await.is_err());
    bound.release(&release_context(&context)).await.unwrap();
    assert!(tool.execute(&arguments, &execution).await.is_err());
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        1
    );
    let mut next = context.clone();
    next.binding_set_id = id("fresh-segment");
    let next_bound = runtime.bind(&assembly, &next).await.unwrap();
    assert!(
        next_bound
            .tools()
            .get(&id("search_records"))
            .unwrap()
            .executor
            .execute(&arguments, &execution_context(&next))
            .await
            .is_ok()
    );
    assert!(
        tool.execute(&arguments, &execution_context(&next))
            .await
            .is_err()
    );
    next_bound.release(&release_context(&next)).await.unwrap();
    assert_eq!(fixture.factories[0].instances.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn two_bindings_of_the_same_export_keep_separate_instances_and_arguments() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let selected = multi_profile(&["adapter", "adapter"]);
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    for index in 0..2 {
        let name = id(&format!("search_{index}"));
        let tool = bound.tools().get(&name).unwrap();
        assert_eq!(bound.tools().selection(&name), Some(&selected.tools[index]));
        let arguments = object(
            json!({"query":format!("binding-{index}"),"workspace_id":"11111111-1111-4111-8111-111111111111"}),
        );
        tool.executor
            .execute(&arguments, &execution_context(&context))
            .await
            .unwrap();
    }
    {
        let instances = fixture.factories[0].instances.lock().unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(
            instances[0].executor.seen.lock().unwrap()[0].0["query"],
            json!("binding-0")
        );
        assert_eq!(
            instances[1].executor.seen.lock().unwrap()[0].0["query"],
            json!("binding-1")
        );
    }
    bound.release(&release_context(&context)).await.unwrap();
}

#[tokio::test]
async fn initialization_requires_current_permission_and_opens_only_after_it_is_granted() {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    fixture.policy.deny_open.store(1, Ordering::SeqCst);
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    fixture.policy.deny_open.store(0, Ordering::SeqCst);
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 1);
    bound.release(&release_context(&context)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_later_factory_still_releases_the_already_open_instance() {
    let mut fixture = Fixture::new();
    fixture.add_adapter("second");
    fixture.factories[1].behavior.store(4, Ordering::SeqCst);
    let selected = multi_profile(&["adapter", "second"]);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture
        .runtime(registry.clone())
        .with_settings(AdapterRuntimeSettings {
            open_timeout_ms: 10,
            close_timeout_ms: 10,
        })
        .unwrap();
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let started = tokio::time::Instant::now();
    assert!(runtime.bind(&assembly, &context).await.is_err());
    assert!(started.elapsed() <= Duration::from_millis(30));
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:binding-0", "open:binding-1", "close:binding-0"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn observer_only_binding_requires_a_committed_outcome_and_never_activates_a_tool_only_adapter()
 {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut observer = context.clone();
    observer.binding_set_id = id("observer-segment");
    observer.purpose = ComponentBindPurpose::ObserversOnly;
    observer.lease = None;
    assert!(runtime.bind(&assembly, &observer).await.is_err());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    let saved = fixture.store.load(&scope(), &context.run_id).await.unwrap();
    let finished = core_fixture::finished(
        &saved.snapshot,
        context.lease.clone().unwrap(),
        fixture.clock.now().unwrap().utc_ms,
    );
    fixture
        .store
        .commit(&scope(), &context.run_id, finished)
        .await
        .unwrap();
    let bound = runtime.bind(&assembly, &observer).await.unwrap();
    let metadata_tool = bound.tools().get(&id("search_records")).unwrap();
    let rejected = metadata_tool
        .executor
        .execute(
            &object(
                json!({"query":"report","workspace_id":"11111111-1111-4111-8111-111111111111"}),
            ),
            &execution_context(&observer),
        )
        .await
        .unwrap();
    assert_eq!(
        rejected.outcome,
        ToolExecutionOutcome::Failed {
            code: id("component_unavailable")
        }
    );
    assert_eq!(rejected.effect, ToolEffect::NotApplied);
    assert!(rejected.receipt.is_none());
    assert_eq!(fixture.factories[0].opens.load(Ordering::SeqCst), 0);
    assert!(
        bound
            .release(&release_context(&observer))
            .await
            .unwrap()
            .failures
            .is_empty()
    );
}

#[tokio::test]
async fn dropping_a_bind_waiter_closes_late_instances_instead_of_publishing_or_leaking_them() {
    let mut fixture = Fixture::new();
    fixture.add_adapter("second");
    fixture.factories[1].behavior.store(7, Ordering::SeqCst);
    let selected = multi_profile(&["adapter", "second"]);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &selected, "run", "segment").await;
    let mut binding = runtime.bind(&assembly, &context);
    assert!(futures_util::poll!(binding.as_mut()).is_pending());
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.factories[1].entered.notified(),
    )
    .await
    .unwrap();
    drop(binding);
    fixture.factories[1].release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.starts_with("close:"))
                .count()
                == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec![
            "open:binding-0",
            "open:binding-1",
            "close:binding-1",
            "close:binding-0"
        ]
    );
    assert!(fixture.factories.iter().all(|factory| {
        factory.instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst)
            == 0
    }));
}

#[tokio::test]
async fn permission_revoked_while_the_factory_opens_prevents_publication_and_closes_the_instance() {
    let fixture = Fixture::new();
    fixture.factories[0].behavior.store(7, Ordering::SeqCst);
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let mut binding = runtime.bind(&assembly, &context);
    assert!(futures_util::poll!(binding.as_mut()).is_pending());
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture.factories[0].entered.notified(),
    )
    .await
    .unwrap();
    fixture.policy.deny_open.store(1, Ordering::SeqCst);
    fixture.factories[0].release.add_permits(1);
    assert!(binding.await.is_err());
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec!["open:records", "close:records"]
    );
    assert_eq!(
        fixture.factories[0].instances.lock().unwrap()[0]
            .executor
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn reconciliation_preserves_the_original_attempt_without_reentering_a_released_or_foreign_binding()
 {
    let fixture = Fixture::new();
    let registry = Arc::new(fixture.registry().unwrap());
    let runtime = fixture.runtime(registry.clone());
    let (assembly, context) = fixture.admit(&registry, &profile(), "run", "segment").await;
    let bound = runtime.bind(&assembly, &context).await.unwrap();
    let tool = &bound.tools().get(&id("search_records")).unwrap().executor;
    let args: JsonObject =
        serde_json::from_value(json!({"workspace_id":"recorded-target","query":"original query"}))
            .unwrap();
    let original = execution_context(&context);
    let result = tool.execute(&args, &original).await.unwrap();
    let executor = fixture.factories[0].instances.lock().unwrap()[0]
        .executor
        .clone();
    let mut current = original.clone();
    current.principal_ref = id("recovery-reviewer");
    let observed = tool.reconcile(&args, &current).await.unwrap();
    assert_eq!(observed, ToolReconciliation::Known { result });
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let queries = executor.reconciliations.load(Ordering::SeqCst);
    for kind in [0, 1, 2] {
        let mut foreign = current.clone();
        match kind {
            0 => foreign.scope.tenant_id = id("foreign"),
            1 => foreign.run_id = id("different-run"),
            _ => foreign.binding_set_id = Some(id("different-segment")),
        };
        assert_eq!(
            tool.reconcile(&args, &foreign).await.unwrap_err().code,
            ErrorCode::AccessDenied
        );
    }
    assert_eq!(executor.reconciliations.load(Ordering::SeqCst), queries);
    bound.release(&release_context(&context)).await.unwrap();
    assert_eq!(
        tool.reconcile(&args, &current).await.unwrap_err().code,
        ErrorCode::InvalidTransition
    );
    assert_eq!(executor.reconciliations.load(Ordering::SeqCst), queries);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
}
