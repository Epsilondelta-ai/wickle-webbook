use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: reference.version.clone().or_else(|| Some(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
fn compiled_tool() -> Result<CompiledTool, ContractError> {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])?;
    SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search available evidence".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","description":"Internal workspace key"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"array"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &registry)
}
fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference("primary"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("example"),
        model_id: id("example"),
        model_version: id("1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("example"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        adapter: reference("example"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    }
}
fn request(run: &str, text: &str) -> RunRequest {
    RunRequest {
        request_id: id(&format!("request-{run}")),
        session_id: id("session"),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        max_output_tokens: None,
        output_contract: None,
    }
}
fn admission(
    profile: &ResolvedProfile,
    prompt: &ProtectedRecord,
    run: &str,
    first_sequence: u64,
    started_at_ms: i64,
    text: &str,
) -> AdmissionInput {
    let request = request(run, text);
    let request_record = ProtectedRecord::new(
        id(&format!("input-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let scope = profile.scope().clone();
    let snapshot = RunSnapshot {
        interruption_plan_ref: None,
        interruption_records: vec![],
        app_state: None,
        model_step_inputs: vec![],
        prepared_steps: vec![],
        active_prepared_step: None,
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, profile, None),
        request: request.clone(),
        scope: scope.clone(),
        profile: profile.clone(),
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        limits: profile.profile().limits.clone(),
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at_ms, 10000).unwrap(),
        reservations: vec![],
        resume_receipts: vec![], recovery_receipts: vec![],
        hook_plan_ref: None, source_plan_ref: None, skill_plan_ref: None, context_plan_ref: None, context_revision_ref: None, context_decisions: vec![], verification_plan_ref: None, candidate_ref: None, verification_records: vec![],
        hook_applications: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        routing_snapshot_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    AdmissionInput {
        execution_principal_ref: id("execution-principal"),
                execution_grant_ref: id("execution-grant"),
        submitted: None,
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        require_durable: false,
        messages: vec![Message {
            source_model_request_id: None,
            message_id: id(&format!("user-{run}")),
            run_id: id(run),
            sequence: first_sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }],
        events: vec![RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: id(&format!("start-{run}")),
            scope,
            run_id: id(run),
            session_id: id("session"),
            seq: 1.try_into().unwrap(),
            timestamp_ms: started_at_ms,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: profile.profile_digest().clone(),
            },
        }],
        records: vec![request_record, prompt.clone()],
    }
}
fn project(
    prompt: &PromptSnapshot,
    stored: &StoredRun,
) -> Result<ContextProjection, ContractError> {
    let step = id(&format!("step-{}", stored.snapshot.run_id));
    ContextAssembler::new().project(
        prompt,
        ProjectionInput {
            tool_contracts: &[],
            profile: &stored.snapshot.profile,
            scope: &stored.snapshot.scope,
            run_id: &stored.snapshot.run_id,
            model_step_id: &step,
            current_request: &stored.snapshot.request,
            current_request_message_id: &id(&format!("user-{}", stored.snapshot.run_id)),
            transcript: &stored.messages,
            context_items: &[],
            opaque_records: &[],
            expected_prompt_digest: &stored.session.prompt_snapshot.digest,
            request_id: step.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            options: stored.snapshot.request.model_options.clone(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 32_768,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 16,
                max_tool_calls: 1,
            },
            limits: ProjectionLimits {
                max_bytes: 32_768,
                max_items: 30,
            },
        },
    )
}

fn projected_user_occurrences(projection: &ContextProjection, text: &str) -> usize {
    projection
        .request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .flat_map(|message| &message.content)
        .filter(|content| matches!(content, ModelContent::Text { text: value } if value == text))
        .count()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Context example","instructions":{"text":"Summarize available evidence"},
      "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let tool = compiled_tool()?;
    let prompt = PromptSnapshot::create(
        &profile,
        vec!["Only report actions supported by supplied observations.".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool.clone(),
        }],
        vec![],
    )?;
    let prompt_record = ProtectedRecord::new(id("prompt"), 1, serde_json::to_value(&prompt)?);
    assert_eq!(prompt.digest(), prompt_record.reference().digest);
    let store = MemoryStateStore::new();
    let mut first_input = admission(
        &profile,
        &prompt_record,
        "first",
        1,
        1000,
        "Review the available evidence",
    );
    first_input.messages.push(Message {
        source_model_request_id: None,
        message_id: id("private-state"),
        run_id: id("first"),
        sequence: 2.try_into()?,
        role: MessageRole::System,
        content: vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"workspace_id":"host-only-database-value"}),
            },
        }],
        origin: MessageOrigin::Host,
        visibility: Visibility::Internal,
    });
    let first = store.admit(&scope, first_input).await?.state;
    let first_projection = project(&prompt, &first)?;
    assert_eq!(first_projection.request.options, first.snapshot.request.model_options);
    assert_eq!(first_projection.request.messages[0].role, ModelRole::System);
    assert_eq!(
        projected_user_occurrences(&first_projection, "Review the available evidence"),
        1
    );
    assert!(
        first_projection.request.tools[0].model_input_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        !serde_json::to_string(&first_projection.request)?.contains("host-only-database-value")
    );
    assert_eq!(
        first_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-first"))
            .count(),
        1
    );

    // Finish this demonstration run without claiming that a model or tool executed.
    let lease = store
        .acquire_lease(&scope, &id("first"), &id("worker"), 1000, 1000)
        .await?;
    let mut snapshot = first.snapshot.clone();
    snapshot.revision = 1;
    snapshot.last_event_seq = 2;
    snapshot.status = RunStatus::Cancelled;
    snapshot.phase = RunPhase::Finish;
    snapshot.usage.elapsed_ms = 1;
    snapshot.timing.last_observed_at_ms = 1001;
    let outcome = RunOutcome {
        app_state: None,
        result: OutcomeResult::Cancelled {
            reason: "Demonstration complete".into(),
        },
        output: vec![],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record =
        ProtectedRecord::new(id("outcome-first"), 1, serde_json::to_value(&outcome)?);
    snapshot.outcome = Some(outcome);
    store
        .commit(
            &scope,
            &id("first"),
            CommitInput {
                control_commands: vec![],
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot,
                messages: vec![],
                records: vec![outcome_record.clone()],
                events: vec![RunEvent {
                    schema_version: RunEventSchemaVersion::V1,
                    event_id: id("finish-first"),
                    scope: scope.clone(),
                    run_id: id("first"),
                    session_id: id("session"),
                    seq: 2.try_into()?,
                    timestamp_ms: 1001,
                    payload: RunEventPayload::RunFinished {
                        outcome_ref: outcome_record.reference().clone(),
                    },
                }],
            },
        )
        .await?;

    let changed = PromptSnapshot::create(
        &profile,
        vec!["Changed operating policy".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool,
        }],
        vec![],
    )?;
    let changed_record =
        ProtectedRecord::new(id("changed-prompt"), 1, serde_json::to_value(&changed)?);
    assert!(
        store
            .admit(
                &scope,
                admission(&profile, &changed_record, "wrong", 3, 1002, "Continue")
            )
            .await
            .is_err()
    );
    let second = store
        .admit(
            &scope,
            admission(
                &profile,
                &prompt_record,
                "second",
                3,
                1002,
                "Now give a concise summary",
            ),
        )
        .await?
        .state;
    let saved_prompt = store
        .read_record(&scope, &second.session.prompt_snapshot)
        .await?;
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(saved_prompt.value())?,
        &second.session.prompt_snapshot.digest,
        &second.snapshot.profile,
        &scope,
    )?;
    let second_projection = project(&restored, &second)?;
    assert_eq!(
        projected_user_occurrences(&second_projection, "Now give a concise summary"),
        1
    );
    assert_eq!(
        first_projection.prompt_digest,
        second_projection.prompt_digest
    );
    assert_eq!(
        first_projection.request.tools,
        second_projection.request.tools
    );
    assert_eq!(
        second_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-second"))
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&second_projection.request)?.contains("host-only-database-value")
    );
    println!(
        "context consumer: two stored runs share the pinned prompt/tool schema; changed prompt refused; current request appears once; internal execution data excluded"
    );
    Ok(())
}
