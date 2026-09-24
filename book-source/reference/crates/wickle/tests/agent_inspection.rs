//! Stored diagnostics disclose evidence without preparing or executing another step.
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
#[allow(dead_code, unused_imports)]
mod source_support;
use agent_support::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

struct InspectionPolicy {
    mode: AtomicUsize,
    content: AtomicUsize,
    fragment_checks: AtomicUsize,
    pause_fragment: AtomicUsize,
    fragment_entered: tokio::sync::Notify,
    fragment_release: tokio::sync::Semaphore,
}
impl Default for InspectionPolicy {
    fn default() -> Self {
        Self {
            mode: AtomicUsize::new(0),
            content: AtomicUsize::new(0),
            fragment_checks: AtomicUsize::new(0),
            pause_fragment: AtomicUsize::new(0),
            fragment_entered: tokio::sync::Notify::new(),
            fragment_release: tokio::sync::Semaphore::new(0),
        }
    }
}
impl PolicyPort for InspectionPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            let mode = match &request.action {
                PolicyAction::InspectStep {
                    context_fragments, ..
                } => {
                    let mode = self.mode.load(Ordering::SeqCst);
                    if mode == 0 && !context_fragments.is_empty() {
                        self.content.load(Ordering::SeqCst)
                    } else {
                        mode
                    }
                }
                PolicyAction::InspectContextFragment { .. } => {
                    let number = self.fragment_checks.fetch_add(1, Ordering::SeqCst) + 1;
                    if number == self.pause_fragment.load(Ordering::SeqCst) {
                        self.fragment_entered.notify_one();
                        self.fragment_release.acquire().await.unwrap().forget();
                    }
                    self.content.load(Ordering::SeqCst)
                }
                _ => 0,
            };
            Ok(match mode {
                1 => PolicyDecision::Deny {
                    reason: id("revoked"),
                },
                2 => PolicyDecision::RequireApproval {
                    reason: id("review"),
                },
                _ => PolicyDecision::Allow {},
            })
        })
    }
}
struct NoExecution;
impl ModelPort for NoExecution {
    fn binding(&self) -> ModelPortBinding {
        panic!("inspection must not resolve a model")
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        panic!("inspection must not invoke a model")
    }
    fn tool_schema_compiler(&self) -> Arc<dyn ProviderToolSchemaCompiler> {
        panic!("inspection must not compile Tool schemas")
    }
}
impl ModelTokenEstimator for NoExecution {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        panic!("inspection must not estimate again")
    }
}
impl ComponentRuntime for NoExecution {
    fn resolve<'a>(
        &'a self,
        _: &'a ResolvedProfile,
        _: &'a ComponentResolveContext,
    ) -> PortFuture<'a, ResolvedAssembly> {
        panic!("inspection must not resolve adapter factories")
    }
    fn bind<'a>(
        &'a self,
        _: &'a ResolvedAssembly,
        _: &'a ComponentBindContext,
    ) -> PortFuture<'a, BoundCapabilities> {
        panic!("inspection must not open adapter factories")
    }
}
fn reader(
    profile: AgentProfile,
    mut bindings: AgentBindings,
    policy: Arc<InspectionPolicy>,
) -> Agent {
    bindings.policy = Arc::new(PolicyGate::new(policy, Duration::from_secs(2)).unwrap());
    bindings.model_exchange = Arc::new(ModelExchange::new(
        Arc::new(NoExecution),
        bindings.policy.clone(),
    ));
    bindings.token_estimator = Arc::new(NoExecution);
    if bindings.tools.is_none() && bindings.context_sources.is_none() {
        bindings.components = Some(Arc::new(NoExecution));
    }
    create_agent(profile, bindings).unwrap()
}
async fn reference(store: &dyn StateStore, run: &Id) -> (StepRef, PreparedStepRecord) {
    let saved = store.load(&scope(), run).await.unwrap();
    let record = store
        .read_record(&scope(), saved.snapshot.prepared_steps.last().unwrap())
        .await
        .unwrap();
    (
        StepRef::Prepared {
            record_id: record.reference().record_id.clone(),
        },
        serde_json::from_value(record.value().clone()).unwrap(),
    )
}
async fn report(agent: &Agent, run: &Id, step: StepRef, raw: bool) -> CompositionReport {
    completed(
        agent
            .inspect_step(
                run,
                step,
                &context(),
                InspectionOptions {
                    include_context_content: raw,
                },
            )
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn inspection_is_reproducible_preserves_fingerprints_and_has_no_execution_or_storage_writes()
{
    let fixture = Fixture::new(Response::Text, false);
    let mut profile = profile();
    profile.model_options = [("effort".into(), serde_json::json!("low"))].into();
    let running = create_agent(profile.clone(), fixture.bindings()).unwrap();
    let mut input = request("inspection");
    input.model_options = [("effort".into(), serde_json::json!("high"))].into();
    let handle = completed(running.start(input, context()).await.unwrap());
    completed(handle.outcome(&context()).await.unwrap());
    let (step, root) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let reader = reader(
        profile,
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    let counters = (
        fixture.model.calls.load(Ordering::SeqCst),
        fixture.catalog.calls.load(Ordering::SeqCst),
        fixture.router.queries.load(Ordering::SeqCst),
    );
    let first = report(&reader, handle.run_id(), step.clone(), false).await;
    let second = report(
        &reader,
        handle.run_id(),
        StepRef::Logical {
            model_step_id: root.model_step_id.clone(),
            purpose: root.purpose,
            projection_revision: root.projection_revision,
        },
        false,
    )
    .await;
    assert_eq!(first.composition, second.composition);
    assert_eq!(first.status, InspectionStatus::Found);
    let composition = first.composition.as_ref().unwrap();
    assert_eq!(composition.fingerprint, root.projection_fingerprint);
    assert_eq!(composition.evidence, vec![InspectionEvidence::Prepared]);
    assert_eq!(
        composition
            .recorded_run_outcome
            .as_ref()
            .unwrap()
            .completion_basis,
        Some(CompletionBasis::TurnEnded)
    );
    assert_eq!(composition.attempts.len(), 1);
    assert!(
        composition.attempts[0]
            .evidence
            .contains(&InspectionEvidence::ResponseObserved)
    );
    let options = composition
        .model
        .as_ref()
        .unwrap()
        .configuration
        .as_ref()
        .unwrap();
    assert_eq!(
        options.effective.get("effort"),
        Some(&serde_json::json!("high"))
    );
    assert_eq!(options.sources.get("effort"), Some(&ModelOptionSource::Run));
    assert!(composition.estimated_input_tokens.unwrap() > 0);
    assert!(composition.estimator.is_none());
    assert!(
        first
            .unresolved
            .iter()
            .any(|field| field.field == "estimator" && field.reason == "not_recorded")
    );
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
    assert_eq!(
        (
            fixture.model.calls.load(Ordering::SeqCst),
            fixture.catalog.calls.load(Ordering::SeqCst),
            fixture.router.queries.load(Ordering::SeqCst)
        ),
        counters
    );
}

#[tokio::test]
async fn tool_report_contains_model_schemas_but_no_bound_uuid_values_or_connection_details() {
    let mut fixture = resume_support::Fixture::new(resume_support::Mode::Approval);
    let mut registrations = vec![];
    for name in ["before", "target", "after"] {
        let registered = fixture.registry.get(&id(name)).unwrap();
        let mut descriptor = registered.compiled.descriptor().clone();
        if name == "target" {
            descriptor.description = "private-description-sentinel".into();
            descriptor.input_schema["properties"]["query"]["default"] =
                serde_json::json!("private-default-sentinel");
        }
        registrations.push(ToolRegistration {
            compiled: SchemaCompiler::new()
                .compile(descriptor, &fixture.inputs)
                .unwrap(),
            executor: registered.executor.clone(),
        });
    }
    fixture.registry = Arc::new(ToolRegistry::new(scope(), registrations).unwrap());
    let handle = fixture.started(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, _) = reference(fixture.base.store.as_ref(), handle.run_id()).await;
    let agent = reader(
        fixture.profile.clone(),
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let calls = fixture.resolver.calls.load(Ordering::SeqCst);
    let result = report(&agent, handle.run_id(), step, true).await;
    let composition = result.composition.as_ref().unwrap();
    assert_eq!(composition.tools.len(), 3);
    let target = composition
        .tools
        .iter()
        .find(|tool| tool.canonical_name == id("target"))
        .unwrap();
    assert!(target.canonical_schema["properties"].get("query").is_some());
    assert!(
        target.canonical_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(target.provider_schema.is_some());
    assert!(target.decode_plan_digest.is_some());
    assert!(
        composition
            .redacted_paths
            .iter()
            .any(|path| path.ends_with("query.default"))
    );
    assert!(target.canonical_schema["properties"]["query"]["default"].is_null());
    let encoded = serde_json::to_string(&result).unwrap();
    for private in [
        resume_support::WORKSPACE,
        resume_support::RECORD,
        "execution_args",
        "system_bindings",
        "connection_ref",
        "private-description-sentinel",
        "private-default-sentinel",
    ] {
        assert!(!encoded.contains(private), "disclosed {private}");
    }
    assert_eq!(fixture.resolver.calls.load(Ordering::SeqCst), calls);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preparation_only_and_an_unconfirmed_reserved_attempt_have_distinct_evidence() {
    for reserved in [false, true] {
        let fixture = Fixture::new(Response::Text, false);
        let mut bindings = fixture.bindings();
        if reserved {
            bindings.state = Arc::new(FinalCommitStore::new(
                fixture.store.clone(),
                FinalCommitMode::RejectModelResult,
            ));
        } else {
            fixture.policy.deny.store(10, Ordering::SeqCst);
        }
        let owner = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "evidence").await;
        let _ = handle.outcome(&context()).await;
        let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
        let agent = reader(
            profile(),
            fixture.bindings(),
            Arc::new(InspectionPolicy::default()),
        );
        let result = report(&agent, handle.run_id(), step, false).await;
        let composition = result.composition.unwrap();
        assert_eq!(composition.evidence, vec![InspectionEvidence::Prepared]);
        assert_eq!(composition.attempts.len(), usize::from(reserved));
        if reserved {
            assert_eq!(
                composition.attempts[0].evidence,
                vec![
                    InspectionEvidence::DispatchReserved,
                    InspectionEvidence::TransmissionUnknown
                ]
            );
            assert_ne!(composition.recorded_run_status, RunStatus::Succeeded);
        }
        assert_eq!(
            fixture.model.calls.load(Ordering::SeqCst),
            usize::from(reserved)
        );
    }
}

#[tokio::test]
async fn missing_and_expired_records_remain_structured_gaps_without_repreparation() {
    let fixture = Fixture::new(Response::Text, false);
    let handle = fixture.started(&fixture.agent(), "retention").await;
    completed(handle.outcome(&context()).await.unwrap());
    let (step, root) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let StepRef::Prepared { record_id } = &step else {
        unreachable!()
    };
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PassThrough,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let agent = reader(profile(), bindings, Arc::new(InspectionPolicy::default()));
    for (code, status) in [
        (ErrorCode::StateNotFound, InspectionStatus::NotFound),
        (ErrorCode::RecordExpired, InspectionStatus::Expired),
    ] {
        *store.record_fault.lock().unwrap() = Some((record_id.clone(), code));
        let result = report(&agent, handle.run_id(), step.clone(), false).await;
        assert_eq!(result.status, status);
        assert!(result.composition.is_none());
    }
    *store.record_fault.lock().unwrap() =
        Some((root.context_projection.record_id, ErrorCode::RecordExpired));
    let partial = report(&agent, handle.run_id(), step, false).await;
    assert_eq!(partial.status, InspectionStatus::Partial);
    assert!(
        partial
            .composition
            .unwrap()
            .estimated_input_tokens
            .is_none()
    );
    assert!(
        partial
            .unresolved
            .iter()
            .any(|field| field.field == "projection" && field.reason == "expired")
    );
    *store.record_fault.lock().unwrap() = None;
    assert_eq!(
        report(
            &agent,
            handle.run_id(),
            StepRef::Prepared {
                record_id: id("unknown-record")
            },
            false
        )
        .await
        .status,
        InspectionStatus::NotFound
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn approval_and_denial_do_not_read_the_store_or_create_a_wait_and_late_revocation_is_respected()
 {
    let fixture = Fixture::new(Response::Text, false);
    let handle = fixture.started(&fixture.agent(), "authorization").await;
    completed(handle.outcome(&context()).await.unwrap());
    let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
    let store = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::PassThrough,
    ));
    let mut bindings = fixture.bindings();
    bindings.state = store.clone();
    let policy = Arc::new(InspectionPolicy::default());
    let agent = reader(profile(), bindings, policy.clone());
    let before = fixture.store.export_checkpoint(&scope()).unwrap();
    policy.mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        agent
            .inspect_step(
                handle.run_id(),
                step.clone(),
                &context(),
                Default::default()
            )
            .await
            .unwrap(),
        Guarded::ApprovalRequired(_)
    ));
    policy.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        agent
            .inspect_step(
                handle.run_id(),
                step.clone(),
                &context(),
                Default::default()
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(store.load_calls.load(Ordering::SeqCst), 0);
    policy.mode.store(0, Ordering::SeqCst);
    store.block_read.store(5, Ordering::SeqCst);
    let run = handle.run_id().clone();
    let querying = tokio::spawn(async move {
        agent
            .inspect_step(&run, step, &context(), Default::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), store.read_entered.notified())
        .await
        .unwrap();
    policy.mode.store(1, Ordering::SeqCst);
    store.release.add_permits(1);
    assert_eq!(
        querying.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
    assert_eq!(
        fixture.store.export_checkpoint(&scope()).unwrap().digest(),
        before.digest()
    );
}

#[tokio::test]
async fn fragment_content_requires_opt_in_and_current_permission_without_source_callbacks() {
    let mut fixture = source_support::Fixture::new();
    let source = fixture.add(
        "search",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    let handle = fixture.start(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, root) = reference(fixture.store.inner.as_ref(), handle.run_id()).await;
    let policy = Arc::new(InspectionPolicy::default());
    let agent = reader(fixture.profile(), fixture.agent_bindings(), policy.clone());
    let calls = (
        source.calls.load(Ordering::SeqCst),
        source.uses.load(Ordering::SeqCst),
        fixture.estimator.calls.load(Ordering::SeqCst),
        fixture.model.inner.calls.load(Ordering::SeqCst),
    );
    let hidden = report(&agent, handle.run_id(), step.clone(), false).await;
    assert!(hidden.composition.as_ref().unwrap().fragments.iter().any(|fragment| matches!(&fragment.disclosure, ContextDisclosure::Redacted { reason } if reason == "not_requested")));
    let shown = report(&agent, handle.run_id(), step.clone(), true).await;
    assert!(
        shown
            .composition
            .as_ref()
            .unwrap()
            .fragments
            .iter()
            .any(|fragment| matches!(fragment.disclosure, ContextDisclosure::Included { .. }))
    );
    policy.content.store(1, Ordering::SeqCst);
    let denied = report(&agent, handle.run_id(), step, true).await;
    assert!(
        denied
            .composition
            .as_ref()
            .unwrap()
            .fragments
            .iter()
            .all(|fragment| !matches!(fragment.disclosure, ContextDisclosure::Included { .. }))
    );
    assert_eq!(
        denied.composition.unwrap().fingerprint,
        root.projection_fingerprint
    );
    assert_eq!(
        (
            source.calls.load(Ordering::SeqCst),
            source.uses.load(Ordering::SeqCst),
            fixture.estimator.calls.load(Ordering::SeqCst),
            fixture.model.inner.calls.load(Ordering::SeqCst)
        ),
        calls
    );
}

struct EmptyResponse {
    binding: ModelPortBinding,
    empty: bool,
}
impl ModelPort for EmptyResponse {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        if self.empty {
            Box::pin(futures_util::stream::empty())
        } else {
            Box::pin(futures_util::stream::once(async {
                Err(ContractError::new(
                    ErrorCode::ModelUnavailable,
                    "local.transport",
                ))
            }))
        }
    }
}
#[tokio::test]
async fn local_empty_stream_or_transport_failure_is_not_proof_of_a_provider_response() {
    for empty in [false, true] {
        let fixture = Fixture::new(Response::TransportFailure, false);
        let mut bindings = fixture.bindings();
        bindings.model_exchange = Arc::new(
            ModelExchange::new(
                Arc::new(EmptyResponse {
                    binding: fixture.model.binding(),
                    empty,
                }),
                bindings.policy.clone(),
            )
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
        );
        let owner = create_agent(profile(), bindings).unwrap();
        let handle = fixture.started(&owner, "local-failure").await;
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap())
                .result
                .status(),
            RunStatus::Failed
        );
        let (step, _) = reference(fixture.store.as_ref(), handle.run_id()).await;
        let reader = reader(
            profile(),
            fixture.bindings(),
            Arc::new(InspectionPolicy::default()),
        );
        let report = report(&reader, handle.run_id(), step, false).await;
        let attempt = &report.composition.unwrap().attempts[0];
        assert!(attempt.result_recorded);
        assert!(
            attempt
                .evidence
                .contains(&InspectionEvidence::TransmissionUnknown)
        );
        assert!(
            !attempt
                .evidence
                .contains(&InspectionEvidence::ResponseObserved)
        );
    }
}

#[tokio::test]
async fn raw_fragment_set_is_reauthorized_after_later_fragment_checks_revoke_earlier_access() {
    let mut fixture = source_support::Fixture::new();
    fixture.add(
        "first",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    fixture.add(
        "second",
        ContextTrigger::RunStart,
        true,
        vec![source_support::Reply::Ready],
    );
    let handle = fixture.start(&fixture.agent()).await;
    fixture.outcome(&handle).await;
    let (step, _) = reference(fixture.store.inner.as_ref(), handle.run_id()).await;
    let policy = Arc::new(InspectionPolicy::default());
    policy.pause_fragment.store(2, Ordering::SeqCst);
    let agent = reader(fixture.profile(), fixture.agent_bindings(), policy.clone());
    let run = handle.run_id().clone();
    let pending = tokio::spawn(async move {
        agent
            .inspect_step(
                &run,
                step,
                &context(),
                InspectionOptions {
                    include_context_content: true,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), policy.fragment_entered.notified())
        .await
        .unwrap();
    policy.content.store(1, Ordering::SeqCst);
    policy.fragment_release.add_permits(1);
    assert_eq!(
        pending.await.unwrap().unwrap_err().code,
        ErrorCode::AccessDenied
    );
}

#[tokio::test]
async fn a_step_must_belong_to_the_requested_run_and_scope_and_opaque_data_is_never_disclosed() {
    let fixture = Fixture::new(Response::WithContinuation, false);
    let owner = fixture.agent();
    let first = fixture.started(&owner, "first").await;
    completed(first.outcome(&context()).await.unwrap());
    let (step, _) = reference(fixture.store.as_ref(), first.run_id()).await;
    let first_saved = fixture.store.load(&scope(), first.run_id()).await.unwrap();
    let response_ref = first_saved.snapshot.model_ledger[0]
        .response_ref
        .as_ref()
        .unwrap();
    let protected = fixture
        .store
        .read_record(&scope(), response_ref)
        .await
        .unwrap();
    assert!(protected.value().to_string().contains("fixture-signature"));
    let second = fixture.started(&owner, "second").await;
    completed(second.outcome(&context()).await.unwrap());
    let agent = reader(
        profile(),
        fixture.bindings(),
        Arc::new(InspectionPolicy::default()),
    );
    let visible = report(&agent, first.run_id(), step.clone(), true).await;
    assert!(
        !serde_json::to_string(&visible)
            .unwrap()
            .contains("fixture-signature")
    );
    assert_eq!(
        report(&agent, second.run_id(), step.clone(), false)
            .await
            .status,
        InspectionStatus::NotFound
    );
    let mut foreign = context();
    foreign.data.scope.workspace_id = id("foreign");
    assert_eq!(
        agent
            .inspect_step(first.run_id(), step, &foreign, Default::default())
            .await
            .unwrap_err()
            .code,
        ErrorCode::AccessDenied
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);
}
