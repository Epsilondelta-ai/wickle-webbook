//! Candidate validation, repair, review waits, and durable verification evidence.
#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use wickle::*;

struct Metadata;
impl ProfileResolver for Metadata {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Answers {
    texts: Mutex<VecDeque<String>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}
impl ModelPort for Answers {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let text = self
            .texts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(stream::iter([
            Ok(ModelEvent::TextDelta { text }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: Default::default(),
                continuation: vec![],
            }),
        ]))
    }
}
struct Checks {
    decisions: Mutex<VecDeque<Result<VerificationDecision, ContractError>>>,
    calls: AtomicUsize,
}
impl Verifier for Checks {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("criteria"),
            configuration: Default::default(),
            criteria: "Validate the supplied candidate against the reference fixture.".into(),
        }
    }
    fn verify<'a>(
        &'a self,
        _: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.decisions
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected repeated verifier invocation")
        })
    }
}
fn setup(
    texts: &[&str],
    decisions: Vec<Result<VerificationDecision, ContractError>>,
) -> (
    Fixture,
    AgentProfile,
    AgentBindings,
    Arc<Answers>,
    Arc<Checks>,
) {
    let fixture = Fixture::new(Response::Text, false);
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Metadata);
    let model = Arc::new(Answers {
        texts: Mutex::new(texts.iter().map(|text| (*text).to_owned()).collect()),
        calls: AtomicUsize::new(0),
        requests: Mutex::new(vec![]),
    });
    bindings.model_exchange = Arc::new(
        ModelExchange::new(model.clone(), bindings.policy.clone())
            .with_route_inspector(fixture.inspector.clone(), Duration::from_secs(1))
            .unwrap(),
    );
    let checks = Arc::new(Checks {
        decisions: Mutex::new(decisions.into()),
        calls: AtomicUsize::new(0),
    });
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![checks.clone()],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let mut profile = profile();
    profile.completion_policy = CompletionPolicy::Verified {
        verifier_ref: reference("quality"),
    };
    profile.limits.max_repair_attempts = 2;
    (fixture, profile, bindings, model, checks)
}
#[tokio::test]
async fn pass_pins_candidate_criteria_and_evidence_and_replays_without_verifying_again() {
    let (fixture, profile, bindings, model, checks) =
        setup(&["checked result"], vec![Ok(VerificationDecision::Pass {})]);
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.verification.as_ref().unwrap().verdict,
        VerificationVerdict::Pass
    );
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let summary = outcome.verification.as_ref().unwrap();
    assert_eq!(summary.criteria_ref, reference("criteria"));
    assert_eq!(
        summary.evidence,
        vec![saved.snapshot.candidate_ref.clone().unwrap()]
    );
    let events: Vec<_> = handle.events(0, context()).try_collect().await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "verification.completed")
            .count(),
        1
    );
    let replay = fixture.started(&agent, "request").await;
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        outcome
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let restored = StateStoreCheckpoint::from_json(
        &String::from_utf8(bytes).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
    let fresh = MemoryStateStore::from_checkpoint(restored);
    assert_eq!(fresh.load(&scope(), handle.run_id()).await.unwrap(), saved);
}
#[tokio::test]
async fn repair_retains_the_candidate_and_verification_provenance_then_accepts_only_the_new_result()
{
    let (fixture, profile, bindings, model, checks) = setup(
        &["incomplete", "complete"],
        vec![
            Ok(VerificationDecision::Revise {
                feedback: "Include the missing evidence.".into(),
            }),
            Ok(VerificationDecision::Pass {}),
        ],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "complete".into()
        }]
    );
    assert_eq!(outcome.usage.repair_attempts, 1);
    assert_eq!(outcome.usage.model_calls, 2);
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let feedback: Vec<_> = saved
        .messages
        .iter()
        .filter(|message| message.origin == MessageOrigin::Verification)
        .collect();
    assert_eq!(feedback.len(), 1);
    assert_eq!(feedback[0].visibility, Visibility::Model);
    assert_eq!(
        saved
            .messages
            .iter()
            .filter(|message| message.origin == MessageOrigin::User)
            .count(),
        1
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn repair_budget_prevents_another_model_call_and_preserves_the_partial_candidate() {
    let (fixture, mut profile, bindings, model, checks) = setup(
        &["incomplete"],
        vec![Ok(VerificationDecision::Revise {
            feedback: "Missing evidence.".into(),
        })],
    );
    profile.limits.max_repair_attempts = 0;
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Exhausted {
            budget: BudgetKind::RepairAttempts
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "incomplete".into()
        }]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn quality_rejection_and_verifier_transport_failure_are_distinct() {
    for (decision, expected) in [
        (
            Ok(VerificationDecision::Fail {
                reason: "Evidence contradicts the conclusion.".into(),
            }),
            "verification_failed",
        ),
        (
            Err(ContractError::new(
                ErrorCode::ComponentUnavailable,
                "synthetic.transport",
            )),
            "verification_unavailable",
        ),
    ] {
        let (fixture, profile, bindings, model, _) = setup(&["candidate"], vec![decision]);
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        let OutcomeResult::Failed { failure } = outcome.result else {
            panic!("expected failure")
        };
        assert_eq!(failure.code, id(expected));
        let diagnostic = fixture
            .store
            .read_record(&scope(), failure.diagnostic_ref.as_ref().unwrap())
            .await
            .unwrap();
        if expected == "verification_failed" {
            assert_eq!(
                diagnostic.value()["decision"]["reason"],
                json!("Evidence contradicts the conclusion.")
            );
        } else {
            assert_eq!(
                diagnostic.value()["error"]["code"],
                json!("component_unavailable")
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        if expected == "verification_failed" {
            assert_eq!(
                outcome.verification.unwrap().verdict,
                VerificationVerdict::Fail
            );
        } else {
            assert!(outcome.verification.is_none());
        }
    }
}
#[tokio::test]
async fn review_approval_is_bound_to_the_candidate_and_never_calls_the_model_or_verifier_again() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["review candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Review this evidence.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!("expected review wait")
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!("expected candidate approval")
    };
    assert_eq!(
        waiting.verification.unwrap().verdict,
        VerificationVerdict::Wait
    );
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("approve-review"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let resumed = completed(agent.resume(command.clone(), context()).await.unwrap());
    let outcome = completed(resumed.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "review candidate".into()
        }]
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    let replay = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        completed(replay.outcome(&context()).await.unwrap()),
        outcome
    );
}

fn configure_router(bindings: &mut AgentBindings, json_output: bool, verification: bool) {
    let old = bindings.router.snapshot();
    let mut catalog = old.catalog().clone();
    let mut policy = old.policy().clone();
    if json_output {
        catalog.models[0]
            .capabilities
            .features
            .insert(id("json_output"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("json_output"));
        catalog.bindings[0].evidence.clear();
        let digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        catalog.bindings[0].evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: digest,
            checked_at_ms: 1000,
            evidence_ref: id("updated-fixture"),
            passed: true,
        });
    }
    if verification {
        let mut rule = policy.rules[0].clone();
        rule.purpose = ModelPurpose::Verification;
        policy.rules.push(rule);
    }
    bindings.router = Arc::new(Router {
        snapshot: RoutingSnapshot::new(catalog, policy).unwrap(),
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
}
#[tokio::test]
async fn json_format_and_deterministic_quality_checks_repair_different_failures() {
    let (fixture, mut profile, mut bindings, model, checks) =
        setup(&["not-json", r#"{"amount":5}"#, r#"{"amount":11}"#], vec![]);
    configure_router(&mut bindings, true, false);
    profile.output_contract = OutputContract::JsonSchema {
        schema_ref: reference("shape"),
    };
    let format = json!({"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"],"additionalProperties":false});
    let quality = json!({"type":"object","properties":{"amount":{"type":"integer","minimum":10}},"required":["amount"],"additionalProperties":false});
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![OutputSchemaDefinition {
                schema_ref: reference("shape"),
                schema: format.clone(),
            }],
            vec![Arc::new(
                SchemaVerifier::new(checks.definition(), quality).unwrap(),
            )],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Json {
            value: json!({"amount":11})
        }]
    );
    assert_eq!(outcome.usage.model_calls, 3);
    assert_eq!(outcome.usage.repair_attempts, 2);
    assert_eq!(
        model.requests.lock().unwrap()[0].output,
        ModelOutput::JsonSchema { schema: format }
    );
}
struct ModelReview {
    binding: Id,
}
impl Verifier for ModelReview {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("model-criteria"),
            configuration: serde_json::from_value(json!({"model_binding":self.binding})).unwrap(),
            criteria: "Ask the configured reviewer to check the candidate.".into(),
        }
    }
    fn verify<'a>(
        &'a self,
        input: &'a VerificationInput,
        context: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            let text = context
                .models
                .generate(VerificationModelRequest {
                    stage: id("review"),
                    model_binding: self.binding.clone(),
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: vec![ModelContent::Json {
                            value: json!({"candidate":input.candidate.output}),
                        }],
                    }],
                    options: None,
                    max_output_tokens: 128.try_into().unwrap(),
                })
                .await?;
            serde_json::from_value(parse_json(&text)?)
                .map_err(|_| ContractError::new(ErrorCode::InvalidContract, "review.response"))
        })
    }
}
#[tokio::test]
async fn model_review_shares_budget_and_does_not_replace_the_agent_step() {
    for capacity in [1, 2] {
        let (fixture, mut profile, mut bindings, model, _) =
            setup(&["candidate", r#"{"verdict":"pass"}"#], vec![]);
        configure_router(&mut bindings, false, true);
        profile.limits.max_model_calls = capacity.try_into().unwrap();
        bindings.verification = Some(Arc::new(
            VerificationRuntime::new(
                scope(),
                vec![],
                vec![Arc::new(ModelReview {
                    binding: id("primary"),
                })],
                VerificationLimits::default(),
            )
            .unwrap(),
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        if capacity == 1 {
            assert_eq!(
                outcome.result,
                OutcomeResult::Exhausted {
                    budget: BudgetKind::ModelCalls
                }
            );
        } else {
            assert_eq!(
                outcome.result,
                OutcomeResult::Succeeded {
                    completion_basis: CompletionBasis::Verified
                }
            );
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), capacity as usize);
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        assert_eq!(
            saved.snapshot.model_step_id.as_ref(),
            Some(&saved.snapshot.model_ledger[0].model_step_id)
        );
        assert_eq!(outcome.usage.model_calls, capacity);
        if capacity == 2 {
            assert_eq!(
                saved.snapshot.model_ledger[1].purpose,
                ModelPurpose::Verification
            );
            assert_ne!(
                saved.snapshot.model_ledger[0].model_step_id,
                saved.snapshot.model_ledger[1].model_step_id
            );
        }
    }
}

struct PausedCheck {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl Verifier for PausedCheck {
    fn definition(&self) -> VerifierDefinition {
        VerifierDefinition {
            verifier_ref: reference("quality"),
            criteria_ref: reference("criteria"),
            criteria: "Wait for controlled review completion.".into(),
            configuration: Default::default(),
        }
    }
    fn verify<'a>(
        &'a self,
        _: &'a VerificationInput,
        _: &'a VerifierContext<'a>,
    ) -> PortFuture<'a, VerificationDecision> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            Ok(VerificationDecision::Pass {})
        })
    }
}
#[tokio::test]
async fn cancellation_and_timeout_cannot_adopt_a_late_verifier_pass() {
    for cancelled in [true, false] {
        let (fixture, profile, mut bindings, model, _) = setup(&["candidate"], vec![]);
        let check = Arc::new(PausedCheck {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        bindings.verification = Some(Arc::new(
            VerificationRuntime::new(
                scope(),
                vec![],
                vec![check.clone()],
                VerificationLimits {
                    timeout_ms: if cancelled { 1000 } else { 20 },
                    ..Default::default()
                },
            )
            .unwrap(),
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        check.entered.notified().await;
        if cancelled {
            completed(handle.cancel(id("stop-review"), &context()).await.unwrap());
        }
        let outcome = completed(handle.outcome(&context()).await.unwrap());
        check.release.add_permits(1);
        if cancelled {
            assert_eq!(outcome.result.status(), RunStatus::Cancelled);
        } else {
            assert!(
                matches!(outcome.result,OutcomeResult::Failed{ref failure} if failure.code==id("verification_unavailable"))
            );
        }
        assert!(outcome.verification.is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            completed(handle.outcome(&context()).await.unwrap()),
            outcome
        );
    }
}
#[tokio::test]
async fn decision_commit_failure_keeps_the_candidate_and_ack_loss_does_not_repeat_verification() {
    for lose_ack in [false, true] {
        let (fixture, profile, mut bindings, model, checks) =
            setup(&["candidate"], vec![Ok(VerificationDecision::Pass {})]);
        bindings.state = Arc::new(FinalCommitStore::new(
            fixture.store.clone(),
            if lose_ack {
                FinalCommitMode::LoseVerificationAcknowledgement
            } else {
                FinalCommitMode::RejectVerification
            },
        ));
        let agent = create_agent(profile, bindings).unwrap();
        let handle = fixture.started(&agent, "request").await;
        let outcome = handle.outcome(&context()).await;
        let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
        if lose_ack {
            assert_eq!(
                completed(outcome.unwrap()).result.status(),
                RunStatus::Succeeded
            );
            assert_eq!(saved.snapshot.verification_records.len(), 1);
        } else {
            assert_eq!(outcome.unwrap_err().code, ErrorCode::PersistenceUnavailable);
            assert!(saved.snapshot.candidate_ref.is_some());
            assert!(saved.snapshot.verification_records.is_empty());
            assert_eq!(saved.snapshot.status, RunStatus::Running);
        }
        assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn denied_review_finishes_without_reexecuting_the_candidate() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Human evidence review.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!()
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!()
    };
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("deny-review"),
        action: ResumeAction::Deny {
            wait_id: wait.wait_id,
            target,
            reason: "Evidence rejected.".into(),
        },
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    let outcome = completed(resumed.outcome(&context()).await.unwrap());
    assert!(
        matches!(outcome.result,OutcomeResult::Failed{ref failure} if failure.code==id("verification_failed"))
    );
    assert_eq!(
        outcome.verification.unwrap().verdict,
        VerificationVerdict::Fail
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
}

fn replace_reference(
    value: &mut serde_json::Value,
    old: &serde_json::Value,
    new: &serde_json::Value,
) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                replace_reference(value, old, new)
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                replace_reference(value, old, new)
            }
        }
        _ => {}
    }
}
fn rehash_records(image: &mut serde_json::Value) {
    for _ in 0..image["records"].as_array().unwrap().len() * 2 {
        let changed = image["records"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|record| {
                let digest = serde_json::to_value(canonical_digest(&record["value"])).unwrap();
                if record["reference"]["digest"] == digest {
                    None
                } else {
                    let old = record["reference"].clone();
                    let mut new = old.clone();
                    new["digest"] = digest;
                    Some((old, new))
                }
            });
        let Some((old, new)) = changed else {
            return;
        };
        replace_reference(image, &old, &new);
    }
    panic!("record graph did not converge")
}
#[tokio::test]
async fn recalculating_record_hashes_cannot_replace_the_verified_model_candidate() {
    let (fixture, profile, bindings, _, _) = setup(
        &["original candidate"],
        vec![Ok(VerificationDecision::Pass {})],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    completed(handle.outcome(&context()).await.unwrap());
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    let checkpoint = fixture.store.export_checkpoint(&scope()).unwrap();
    let mut image = serde_json::to_value(checkpoint).unwrap();
    let reference = saved.snapshot.candidate_ref.unwrap();
    let target = image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| record["reference"]["record_id"] == json!(reference.record_id))
        .unwrap();
    target["value"]["output"][0]["text"] = json!("forged candidate");
    rehash_records(&mut image);
    let result =
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image));
    assert!(result.is_err());
}
#[tokio::test]
async fn review_resume_rejects_changed_criteria_and_wrong_candidate_before_any_new_calls() {
    let (fixture, profile, bindings, model, checks) = setup(
        &["candidate"],
        vec![Ok(VerificationDecision::Wait {
            reason: "Review criteria.".into(),
            expires_at_ms: None,
        })],
    );
    let agent = create_agent(profile.clone(), bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let waiting = completed(handle.outcome(&context()).await.unwrap());
    let OutcomeResult::Waiting { wait } = waiting.result else {
        panic!()
    };
    let WaitTarget::Approval { target } = wait.target else {
        panic!()
    };
    let command = ResumeCommand {
        run_id: handle.run_id().clone(),
        expected_revision: waiting.checkpoint_revision,
        command_id: id("review"),
        action: ResumeAction::Approve {
            wait_id: wait.wait_id,
            target,
        },
    };
    let mut wrong = command.clone();
    if let ResumeAction::Approve {
        target: ApprovalTarget::Candidate { candidate_ref, .. },
        ..
    } = &mut wrong.action
    {
        candidate_ref.record_id = id("another-candidate");
    }
    assert!(agent.resume(wrong, context()).await.is_err());
    let mut bindings = fixture.bindings();
    bindings.profile_resolver = Arc::new(Metadata);
    let mut definition = checks.definition();
    definition.configuration.insert("minimum".into(), json!(4));
    let verifier = SchemaVerifier::new(definition, json!({"type":"object"})).unwrap();
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![Arc::new(verifier)],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let changed = create_agent(profile, bindings).unwrap();
    assert_eq!(
        changed.resume(command, context()).await.unwrap_err().code,
        ErrorCode::ContextMismatch
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(checks.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), handle.run_id())
            .await
            .unwrap()
            .snapshot
            .status,
        RunStatus::Waiting
    );
}

#[tokio::test]
async fn a_verifier_decision_cannot_commit_without_its_required_event() {
    let (fixture, profile, mut bindings, _, _) =
        setup(&["candidate"], vec![Ok(VerificationDecision::Pass {})]);
    bindings.state = Arc::new(FinalCommitStore::new(
        fixture.store.clone(),
        FinalCommitMode::OmitVerificationEvent,
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let error = handle.outcome(&context()).await.unwrap_err();
    assert!(matches!(
        error.code,
        ErrorCode::InvalidEvent | ErrorCode::InvalidSnapshot
    ));
    let saved = fixture.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.verification_records.is_empty());
    assert_ne!(saved.snapshot.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn restored_feedback_cannot_be_promoted_to_a_new_user_request() {
    let (fixture, profile, bindings, _, _) = setup(
        &["first", "second"],
        vec![
            Ok(VerificationDecision::Revise {
                feedback: "Add evidence.".into(),
            }),
            Ok(VerificationDecision::Pass {}),
        ],
    );
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    completed(handle.outcome(&context()).await.unwrap());
    let mut image =
        serde_json::to_value(fixture.store.export_checkpoint(&scope()).unwrap()).unwrap();
    fn promote(value: &mut serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                if map.get("origin") == Some(&json!("verification"))
                    && map.contains_key("message_id")
                {
                    map.insert("origin".into(), json!("user"));
                    return true;
                }
                map.values_mut().any(promote)
            }
            serde_json::Value::Array(values) => values.iter_mut().any(promote),
            _ => false,
        }
    }
    assert!(promote(&mut image));
    assert!(
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .is_err()
    );
}

#[tokio::test]
async fn a_verifier_can_use_its_own_explicit_logical_model_binding() {
    let (fixture, profile, mut bindings, model, _) =
        setup(&["candidate", r#"{"verdict":"pass"}"#], vec![]);
    let mut policy = bindings.router.snapshot().policy().clone();
    let mut review = policy.rules[0].clone();
    review.model_binding = id("review-model");
    review.purpose = ModelPurpose::Verification;
    policy.rules.push(review);
    bindings.router = Arc::new(Router {
        snapshot: RoutingSnapshot::new(bindings.router.snapshot().catalog().clone(), policy)
            .unwrap(),
        queries: AtomicUsize::new(0),
        snapshots: AtomicUsize::new(0),
    });
    bindings.verification = Some(Arc::new(
        VerificationRuntime::new(
            scope(),
            vec![],
            vec![Arc::new(ModelReview {
                binding: id("review-model"),
            })],
            VerificationLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(profile, bindings).unwrap();
    let handle = fixture.started(&agent, "request").await;
    let outcome = completed(handle.outcome(&context()).await.unwrap());
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::Verified
        }
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}
