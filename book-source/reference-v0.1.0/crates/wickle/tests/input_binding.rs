//! Frozen system inputs, scoped resolution, defaults, and binding persistence boundaries.

use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use wickle::*;

#[allow(dead_code)]
mod support;
use support::{id, scope};

const OWNED: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn inputs(value: Value) -> SystemInputs {
    SystemInputs::new(object(value))
}
fn versioned(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn definition(key: &str, schema: Value) -> SystemInputDefinition {
    SystemInputDefinition {
        key: id(key),
        version: id("1"),
        value_schema: schema,
        source: SystemInputSource::Run {},
    }
}
fn registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![
        definition("workspace_id", json!({"type":"string","format":"uuid"})),
        definition("query", json!({"type":"string"})),
        definition("user_id", json!({"type":"string"})),
    ])
    .unwrap()
}
fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        tool: versioned("search"),
        name: id("search"),
        description: "Search records".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1,"default":10},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into(), "limit".into()],
        system_bindings: None,
        output_schema: json!({"type":"string"}),
        side_effect: ToolSideEffect::ReadOnly,
        concurrency: ToolConcurrency::Serial,
        retry: ToolRetryPolicy::Never,
        reconcile: false,
        max_output_bytes: 4096.try_into().unwrap(),
    }
}

#[derive(Default)]
struct FakeClock {
    reading: Mutex<(i64, u64)>,
    changed: Notify,
}
impl FakeClock {
    fn advance(&self, millis: u64) {
        *self.reading.lock().unwrap() = (millis as i64, millis);
        self.changed.notify_waiters();
    }
}
impl Clock for FakeClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let (utc_ms, monotonic_ms) = *self.reading.lock().unwrap();
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }
    fn sleep_until<'a>(&'a self, deadline: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now()?.monotonic_ms >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}
#[derive(Default)]
struct Ids(AtomicUsize);
impl IdSource for Ids {
    fn next_id(&self) -> Result<Id, ContractError> {
        Ok(id(&format!(
            "binding-{}",
            self.0.fetch_add(1, Ordering::SeqCst)
        )))
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
                    version: Some(id("1")),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("catalog")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: Default::default(),
                required_capabilities: Default::default(),
                required_connections: Default::default(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

#[derive(Default)]
struct Policy {
    // 0 allow; 1 target ownership; 2 deny lookup; 3 approve lookup; 4 deny execution; 5 approve execution.
    mode: AtomicUsize,
    lookups: AtomicUsize,
    executions: Mutex<Vec<JsonObject>>,
}
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            match &request.action {
                PolicyAction::ResolveSystemInput { .. } => {
                    self.lookups.fetch_add(1, Ordering::SeqCst);
                    match self.mode.load(Ordering::SeqCst) {
                        2 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("lookup_denied"),
                            });
                        }
                        3 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("lookup_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                PolicyAction::ExecuteTool { input } => {
                    self.executions
                        .lock()
                        .unwrap()
                        .push(input.execution_args().clone());
                    match self.mode.load(Ordering::SeqCst) {
                        1 => {
                            // The Host resource catalog associates each real UUID with an owner.
                            let target = input
                                .execution_args()
                                .get("workspace_id")
                                .and_then(Value::as_str);
                            let owner = match target {
                                Some(OWNED) => Some(scope()),
                                Some(OTHER) => Some(Scope {
                                    tenant_id: id("foreign-tenant"),
                                    ..scope()
                                }),
                                _ => None,
                            };
                            if owner.as_ref() != Some(context.scope) {
                                return Ok(PolicyDecision::Deny {
                                    reason: id("target_not_owned"),
                                });
                            }
                        }
                        4 => {
                            return Ok(PolicyDecision::Deny {
                                reason: id("revoked"),
                            });
                        }
                        5 => {
                            return Ok(PolicyDecision::RequireApproval {
                                reason: id("execution_approval"),
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            Ok(PolicyDecision::Allow {})
        })
    }
}

struct Resolver {
    value: Mutex<Option<ResolvedSystemInput>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<SystemInputResolveRequest>>,
    cancel: Mutex<Option<CancellationToken>>,
    advance: Mutex<Option<Arc<FakeClock>>>,
}
impl Resolver {
    fn new(value: Value) -> Self {
        Self {
            value: Mutex::new(Some(ResolvedSystemInput {
                value,
                revision: id("revision-1"),
            })),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            cancel: Mutex::new(None),
            advance: Mutex::new(None),
        }
    }
    fn set(&self, value: Value, revision: &str) {
        *self.value.lock().unwrap() = Some(ResolvedSystemInput {
            value,
            revision: id(revision),
        });
    }
}
impl SystemInputResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        request: &'a SystemInputResolveRequest,
        context: &'a SystemInputResolveContext,
    ) -> PortFuture<'a, Option<ResolvedSystemInput>> {
        Box::pin(async move {
            assert_eq!(context.scope, scope());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            if let Some(token) = self.cancel.lock().unwrap().as_ref() {
                token.cancel();
            }
            if let Some(clock) = self.advance.lock().unwrap().as_ref() {
                clock.advance(10_000);
            }
            Ok(self.value.lock().unwrap().clone())
        })
    }
}
fn resolver_registry() -> SystemInputRegistry {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("current_workspace"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Resolver {
            resolver_ref: versioned("workspace_lookup"),
        },
    }])
    .unwrap()
}
fn resolver_descriptor() -> ToolDescriptor {
    let mut tool = descriptor();
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("current_workspace"),
    )]));
    tool
}

struct Fixture {
    store: Arc<MemoryStateStore>,
    clock: Arc<FakeClock>,
    ids: Arc<Ids>,
    lease: RunLease,
    context: ExecutionContext,
    registry: Arc<SystemInputRegistry>,
    tool: CompiledTool,
    policy: Arc<Policy>,
}
impl Fixture {
    async fn new(
        tool: ToolDescriptor,
        registry: SystemInputRegistry,
        supplied: Option<SystemInputs>,
    ) -> Self {
        let registry = Arc::new(registry);
        let tool = SchemaCompiler::new().compile(tool, &registry).unwrap();
        let fixed = RunSystemInputs::capture(scope(), supplied.clone(), &registry).unwrap();
        let fixed_record = fixed.to_record(id("run-inputs"), 7);
        let fixed_ref = fixed.snapshot_ref(fixed_record.reference()).unwrap();
        let mut input = support::admission("run", "request", "session", "Read evidence", "1").await;
        let mut profile = input.snapshot.profile.profile().clone();
        profile.tools = vec![ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: tool.descriptor().tool.id.clone(),
            version: tool.descriptor().tool.version.clone(),
            bindings: None,
            config: None,
        })];
        input.snapshot.profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope())
            .await
            .unwrap();
        input.snapshot.system_inputs = Some(fixed_ref);
        input.snapshot.request_digest = admission_digest(
            &input.snapshot.request,
            &input.snapshot.profile,
            input.snapshot.system_inputs.as_ref(),
        );
        let RunEventPayload::RunStarted { profile_digest, .. } = &mut input.events[0].payload
        else {
            unreachable!()
        };
        *profile_digest = input.snapshot.profile.profile_digest().clone();
        input.records.push(fixed_record);
        let store = Arc::new(MemoryStateStore::new());
        store.admit(&scope(), input).await.unwrap();
        let lease = store
            .acquire_lease(&scope(), &id("run"), &id("worker"), 0, 20_000)
            .await
            .unwrap();
        let context = ExecutionContext::new(
            ExecutionContextData {
                scope: scope(),
                principal_ref: id("user"),
                capability_grant_ref: id("grant"),
                trace_context: None,
                system_inputs: supplied,
            },
            CancellationToken::new(),
        );
        Self {
            store,
            clock: Arc::new(FakeClock::default()),
            ids: Arc::new(Ids::default()),
            lease,
            context,
            registry,
            tool,
            policy: Arc::new(Policy::default()),
        }
    }
    async fn plan(&self, call_id: &str, model_inputs: JsonObject) -> ToolCall {
        let saved = self.store.load(&scope(), &id("run")).await.unwrap();
        let call = ToolCall {
            call_id: id(call_id),
            model_request_id: id(&format!("request-{call_id}")),
            provider_call_id: id(&format!("provider-{call_id}")),
            tool_name: self.tool.descriptor().name.clone(),
            model_inputs,
            descriptor_digest: Some(self.tool.descriptor_digest().clone()),
            bound_input_ref: None,
        };
        let record = ProtectedRecord::new(
            id(&format!("planned-{call_id}")),
            1,
            serde_json::to_value(&call).unwrap(),
        );
        let mut update = support::prepared(
            &saved.snapshot,
            self.lease.clone(),
            self.clock.now().unwrap().utc_ms,
        );
        update.snapshot.phase = RunPhase::Tool;
        update.snapshot.tool_ledger.push(ToolLedgerEntry {
            call: call.clone(),
            state: ToolCallState::Planned {},
        });
        update.snapshot.last_event_seq += 1;
        update.events = vec![support::event(
            &id("run"),
            &id("session"),
            &scope(),
            update.snapshot.last_event_seq,
            RunEventPayload::ToolPlanned {
                call_ref: record.reference().clone(),
            },
        )];
        update.messages = vec![Message {
            message_id: id(&format!("call-message-{call_id}")),
            run_id: id("run"),
            sequence: (saved.session.transcript_revision + 1).try_into().unwrap(),
            role: MessageRole::Assistant,
            origin: MessageOrigin::Model,
            visibility: Visibility::UserAndModel,
            content: vec![ContentBlock::ToolCall { call: call.clone() }],
        }];
        update.records = vec![record];
        self.store
            .commit(&scope(), &id("run"), update)
            .await
            .unwrap();
        call
    }
    async fn budget(&self, store: Arc<dyn StateStore>) -> RunBudget {
        RunBudget::attach(
            store,
            self.clock.clone(),
            self.ids.clone(),
            scope(),
            id("run"),
            self.lease.clone(),
            self.context.cancellation.clone(),
        )
        .await
        .unwrap()
    }
    fn binder(&self, resolver: Option<Arc<dyn SystemInputResolver>>) -> InputBinder {
        InputBinder::new(
            self.registry.clone(),
            resolver,
            Arc::new(PolicyGate::new(self.policy.clone(), Duration::from_secs(1)).unwrap()),
            self.ids.clone(),
        )
    }
    async fn bind(
        &self,
        call_id: &str,
        resolver: Option<Arc<dyn SystemInputResolver>>,
    ) -> Result<ToolBindingResult, ContractError> {
        self.binder(resolver)
            .bind(
                &self.tool,
                &id(call_id),
                &self.context,
                &self.budget(self.store.clone()).await,
            )
            .await
    }
}

#[test]
fn run_capture_rejects_unregistered_keys_invalid_values_and_resolver_source_conflicts() {
    let invalid_value = RunSystemInputs::capture(
        scope(),
        Some(inputs(json!({"workspace_id":"private-invalid-value"}))),
        &registry(),
    )
    .unwrap_err();
    assert_eq!(invalid_value.path, r#"system_inputs["workspace_id"]"#);
    for value in [
        json!({"unregistered":"x"}),
        json!({"workspace_id":"not-a-uuid"}),
        json!({"workspace_id":null}),
    ] {
        assert!(RunSystemInputs::capture(scope(), Some(inputs(value)), &registry()).is_err());
    }
    assert!(
        RunSystemInputs::capture(
            scope(),
            Some(inputs(json!({"current_workspace":OWNED}))),
            &resolver_registry()
        )
        .is_err()
    );
}

#[test]
fn restored_run_inputs_distinguish_omission_from_empty_or_changed_resume_values() {
    let registry = registry();
    let supplied = inputs(json!({"workspace_id":OWNED}));
    let fixed = RunSystemInputs::capture(scope(), Some(supplied.clone()), &registry).unwrap();
    let record = fixed.to_record(id("snapshot"), 1);
    let reference = fixed.snapshot_ref(record.reference()).unwrap();
    let restored = RunSystemInputs::restore(&record, &reference, &scope(), &registry).unwrap();
    restored.validate_resume(None).unwrap();
    restored.validate_resume(Some(&supplied)).unwrap();
    assert!(
        restored
            .validate_resume(Some(&SystemInputs::default()))
            .is_err()
    );
    assert!(
        restored
            .validate_resume(Some(&inputs(json!({"workspace_id":OTHER}))))
            .is_err()
    );
    assert_eq!(restored.values(), &object(json!({"workspace_id":OWNED})));
    let other_scope = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(RunSystemInputs::restore(&record, &reference, &other_scope, &registry).is_err());
    let mut changed_values = record.value().clone();
    *changed_values
        .get_mut("values")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    let wrong_record = ProtectedRecord::new(id("snapshot"), 1, changed_values);
    assert!(RunSystemInputs::restore(&wrong_record, &reference, &scope(), &registry).is_err());
    let mut changed_definitions = fixed.definitions().values().cloned().collect::<Vec<_>>();
    changed_definitions
        .iter_mut()
        .find(|definition| definition.key == id("workspace_id"))
        .unwrap()
        .version = id("2");
    let changed_registry = SystemInputRegistry::new(changed_definitions).unwrap();
    assert!(RunSystemInputs::restore(&record, &reference, &scope(), &changed_registry).is_err());
}

#[tokio::test]
async fn binding_keeps_original_and_defaulted_model_inputs_separate_from_system_arguments() {
    let fixture=Fixture::new(descriptor(),registry(),Some(inputs(json!({"workspace_id":OWNED,"query":"system query must not overwrite","user_id":"extra-registered-value"})))).await;
    let original = object(json!({"query":"model query"}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs(), &original);
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":"model query","limit":10}))
    );
    assert_eq!(
        bound.input.execution_args(),
        &object(json!({"query":"model query","limit":10,"workspace_id":OWNED}))
    );
    assert_eq!(bound.decision, PolicyDecision::Allow {});
    assert_eq!(
        bound.input.system_inputs()["workspace_id"].definition_version,
        id("1")
    );
    assert_eq!(
        bound.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("7")
    );
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert_eq!(saved.tool_ledger[0].call.model_inputs, original);
    assert_eq!(
        saved.tool_ledger[0].call.bound_input_ref.as_ref(),
        Some(&bound.reference)
    );
    assert_eq!(saved.usage.tool_attempts, 0);
    assert_eq!(
        fixture.policy.executions.lock().unwrap().as_slice(),
        &[object(
            json!({"query":"model query","limit":10,"workspace_id":OWNED})
        )]
    );
}

#[tokio::test]
async fn a_model_supplied_hidden_uuid_is_rejected_even_when_it_matches_the_saved_value() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","workspace_id":OWNED})))
        .await;
    let before = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(fixture.bind("call", None).await.is_err());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        before
    );
}

#[tokio::test]
async fn a_missing_required_system_value_is_not_filled_from_defaults_or_generated_ids() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["workspace_id"]["default"] = json!(OWNED);
    tool.system_bindings = Some(BTreeMap::from([(
        "workspace_id".into(),
        id("active_workspace_id"),
    )]));
    let registry = SystemInputRegistry::new(vec![definition(
        "active_workspace_id",
        json!({"type":"string","format":"uuid"}),
    )])
    .unwrap();
    let fixture = Fixture::new(tool, registry, None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let error = fixture.bind("call", None).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::SystemInputMissing);
    assert_eq!(error.path, r#"system_inputs["active_workspace_id"]"#);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_missing_system_inputs_are_omitted_and_explicit_null_follows_the_schema() {
    for supplied in [None, Some(inputs(json!({"workspace_id":null})))] {
        let mut tool = descriptor();
        tool.input_schema["required"] = json!(["query"]);
        tool.input_schema["properties"]["workspace_id"]["type"] = json!(["string", "null"]);
        let registry = SystemInputRegistry::new(vec![definition(
            "workspace_id",
            json!({"type":["string","null"],"format":"uuid"}),
        )])
        .unwrap();
        let is_null = supplied.is_some();
        let fixture = Fixture::new(tool, registry, supplied).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await.unwrap();
        if is_null {
            assert_eq!(
                bound.input.execution_args().get("workspace_id"),
                Some(&Value::Null)
            );
        } else {
            assert!(!bound.input.execution_args().contains_key("workspace_id"));
        }
    }
}

#[tokio::test]
async fn model_defaults_follow_local_references_but_do_not_invent_nested_fields() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"] = json!({"$ref":"#/$defs/Limit"});
    tool.input_schema["$defs"] = json!({"Limit":{"type":"integer","minimum":1,"default":7}});
    tool.input_schema["properties"]["query"] = json!({"type":"object","properties":{"sort":{"type":"string","default":"descending"}},"additionalProperties":false});
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":{}}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(
        bound.input.original_model_inputs(),
        &object(json!({"query":{}}))
    );
    assert_eq!(
        bound.input.normalized_model_inputs(),
        &object(json!({"query":{},"limit":7}))
    );
}

#[tokio::test]
async fn a_valid_foreign_uuid_is_denied_using_the_actual_bound_target() {
    for target in [OWNED, OTHER] {
        let fixture = Fixture::new(
            descriptor(),
            registry(),
            Some(inputs(json!({"workspace_id":target}))),
        )
        .await;
        fixture.policy.mode.store(1, Ordering::SeqCst);
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let bound = fixture.bind("call", None).await;
        assert_eq!(bound.is_ok(), target == OWNED);
        assert_eq!(
            fixture.policy.executions.lock().unwrap()[0]["workspace_id"],
            json!(target)
        );
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        assert_eq!(
            saved.tool_ledger[0].call.bound_input_ref.is_some(),
            target == OWNED
        );
    }
}

#[tokio::test]
async fn one_resolver_key_is_resolved_once_per_binding_and_cached_calls_keep_the_old_target() {
    let mut tool = resolver_descriptor();
    tool.input_schema["properties"]["owner_workspace_id"] =
        json!({"type":"string","format":"uuid"});
    tool.input_schema["required"] = json!(["query", "workspace_id", "owner_workspace_id"]);
    tool.system_bindings
        .as_mut()
        .unwrap()
        .insert("owner_workspace_id".into(), id("current_workspace"));
    let fixture = Fixture::new(tool, resolver_registry(), None).await;
    fixture.plan("first", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        first.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(
        first.input.execution_args()["owner_workspace_id"],
        json!(OWNED)
    );
    resolver.set(json!(OTHER), "revision-2");
    let cached = fixture.bind("first", Some(resolver.clone())).await.unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cached.reference, first.reference);
    assert_eq!(cached.input.binding_digest(), first.input.binding_digest());
    assert_eq!(cached.input.execution_args()["workspace_id"], json!(OWNED));
    fixture.plan("second", object(json!({"query":"x"}))).await;
    let second = fixture
        .bind("second", Some(resolver.clone()))
        .await
        .unwrap();
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.input.execution_args()["workspace_id"], json!(OTHER));
    assert_eq!(
        second.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-2")
    );
    assert_ne!(second.input.binding_digest(), first.input.binding_digest());
}

#[tokio::test]
async fn cached_bindings_still_recheck_current_permission_without_resolving_again() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    fixture.policy.mode.store(4, Ordering::SeqCst);
    assert!(fixture.bind("call", Some(resolver.clone())).await.is_err());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy.executions.lock().unwrap().len(), 2);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .as_ref(),
        Some(&first.reference)
    );
}

#[tokio::test]
async fn lookup_denial_or_approval_blocks_resolution_while_execution_approval_saves_a_fixed_candidate()
 {
    for mode in [2, 3, 5] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        fixture.policy.mode.store(mode, Ordering::SeqCst);
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture.bind("call", Some(resolver.clone())).await;
        let saved = fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot;
        if mode == 5 {
            let binding = result.unwrap();
            assert!(matches!(
                binding.decision,
                PolicyDecision::RequireApproval { .. }
            ));
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_some());
        } else {
            assert!(result.is_err());
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
            assert!(saved.tool_ledger[0].call.bound_input_ref.is_none());
        }
    }
}

#[tokio::test]
async fn cancellation_deadline_and_lease_loss_block_new_resolver_calls() {
    for stop in [
        ErrorCode::Cancelled,
        ErrorCode::DeadlineExceeded,
        ErrorCode::LeaseLost,
    ] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let budget = fixture.budget(fixture.store.clone()).await;
        match stop {
            ErrorCode::Cancelled => fixture.context.cancellation.cancel(),
            ErrorCode::DeadlineExceeded => fixture.clock.advance(10_000),
            ErrorCode::LeaseLost => fixture
                .store
                .release_lease(&scope(), &id("run"), &fixture.lease, 0)
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        let result = fixture
            .binder(Some(resolver.clone()))
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await;
        assert_eq!(result.unwrap_err().code, stop);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

#[tokio::test]
async fn a_stop_during_resolution_prevents_binding_persistence_and_final_authorization() {
    for cancel in [true, false] {
        let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
        fixture.plan("call", object(json!({"query":"x"}))).await;
        let resolver = Arc::new(Resolver::new(json!(OWNED)));
        if cancel {
            *resolver.cancel.lock().unwrap() = Some(fixture.context.cancellation.clone());
        } else {
            *resolver.advance.lock().unwrap() = Some(fixture.clock.clone());
        }
        let result = fixture.bind("call", Some(resolver.clone())).await;
        assert!(result.is_err());
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert!(fixture.policy.executions.lock().unwrap().is_empty());
        assert!(
            fixture
                .store
                .load(&scope(), &id("run"))
                .await
                .unwrap()
                .snapshot
                .tool_ledger[0]
                .call
                .bound_input_ref
                .is_none()
        );
    }
}

struct FailingCommitStore {
    inner: Arc<MemoryStateStore>,
    commits: AtomicUsize,
    persist_first: bool,
}
impl StateStore for FailingCommitStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        self.inner.capabilities()
    }
    fn find_request<'a>(
        &'a self,
        scope: &'a Scope,
        session_id: &'a Id,
        request_id: &'a Id,
    ) -> PortFuture<'a, Option<StoredRun>> {
        self.inner.find_request(scope, session_id, request_id)
    }
    fn admit<'a>(&'a self, s: &'a Scope, input: AdmissionInput) -> PortFuture<'a, AdmissionResult> {
        self.inner.admit(s, input)
    }
    fn load<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, StoredRun> {
        self.inner.load(s, r)
    }
    fn load_session<'a>(&'a self, s: &'a Scope, r: &'a Id) -> PortFuture<'a, SessionSnapshot> {
        self.inner.load_session(s, r)
    }
    fn acquire_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        o: &'a Id,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.acquire_lease(s, r, o, n, t)
    }
    fn check_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.check_lease(s, r, l, n)
    }
    fn renew_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
        t: u64,
    ) -> PortFuture<'a, RunLease> {
        self.inner.renew_lease(s, r, l, n, t)
    }
    fn release_lease<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        l: &'a RunLease,
        n: i64,
    ) -> PortFuture<'a, ()> {
        self.inner.release_lease(s, r, l, n)
    }
    fn read_events<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a Id,
        a: u64,
        n: usize,
    ) -> PortFuture<'a, EventPage> {
        self.inner.read_events(s, r, a, n)
    }
    fn read_record<'a>(
        &'a self,
        s: &'a Scope,
        r: &'a RecordRef,
    ) -> PortFuture<'a, ProtectedRecord> {
        self.inner.read_record(s, r)
    }
    fn commit<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        input: CommitInput,
    ) -> PortFuture<'a, StoredRun> {
        Box::pin(async move {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.persist_first {
                self.inner.commit(scope, run_id, input).await?;
            }
            Err(ContractError::new(
                ErrorCode::PersistenceUnavailable,
                "binding.commit",
            ))
        })
    }
}

#[tokio::test]
async fn failed_storage_does_not_return_a_ready_binding_or_change_the_planned_call() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let original = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: false,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let result = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(result.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot,
        original
    );
}

#[tokio::test]
async fn a_lost_commit_ack_reuses_the_saved_binding_without_resolving_the_new_value() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let store = Arc::new(FailingCommitStore {
        inner: fixture.store.clone(),
        commits: AtomicUsize::new(0),
        persist_first: true,
    });
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let first = fixture
        .binder(Some(resolver.clone()))
        .bind(
            &fixture.tool,
            &id("call"),
            &fixture.context,
            &fixture.budget(store.clone()).await,
        )
        .await;
    assert_eq!(first.unwrap_err().code, ErrorCode::PersistenceUnavailable);
    let saved = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &saved.tool_ledger[0].call;
    let reference = call
        .bound_input_ref
        .clone()
        .expect("commit applied despite the lost acknowledgement");
    let record = fixture
        .store
        .read_record(&scope(), &reference)
        .await
        .unwrap();
    let committed = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        saved.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(committed.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        committed.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );

    resolver.set(json!(OTHER), "revision-2");
    let retried = fixture.bind("call", Some(resolver.clone())).await.unwrap();
    assert_eq!(retried.reference, reference);
    assert_eq!(retried.input.binding_digest(), committed.binding_digest());
    assert_eq!(retried.input.execution_args()["workspace_id"], json!(OWNED));
    assert_eq!(
        retried.input.system_inputs()["workspace_id"]
            .resolved
            .as_ref()
            .unwrap()
            .revision,
        id("revision-1")
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .revision,
        saved.revision
    );
}

#[tokio::test]
async fn restored_bound_inputs_require_the_original_record_call_scope_and_run() {
    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let bound = fixture.bind("call", None).await.unwrap();
    let record = fixture
        .store
        .read_record(&scope(), &bound.reference)
        .await
        .unwrap();
    let snapshot = fixture
        .store
        .load(&scope(), &id("run"))
        .await
        .unwrap()
        .snapshot;
    let call = &snapshot.tool_ledger[0].call;
    let restored = BoundToolInput::restore(
        &record,
        &fixture.tool,
        &scope(),
        &id("run"),
        call,
        snapshot.system_inputs.as_ref(),
    )
    .unwrap();
    assert_eq!(restored.execution_args(), bound.input.execution_args());
    for (record_id, revision) in [
        (id("another-record"), record.reference().revision),
        (
            record.reference().record_id.clone(),
            record.reference().revision + 1,
        ),
    ] {
        let relocated = ProtectedRecord::new(record_id, revision, record.value().clone());
        assert!(
            BoundToolInput::restore(
                &relocated,
                &fixture.tool,
                &scope(),
                &id("run"),
                call,
                snapshot.system_inputs.as_ref()
            )
            .is_err()
        );
    }
    let foreign = Scope {
        tenant_id: id("foreign"),
        ..scope()
    };
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &foreign,
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("another-run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_call = call.clone();
    changed_call.model_inputs = object(json!({"query":"changed"}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            &changed_call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_values = record.value().clone();
    let data = changed_values.get_mut("data").unwrap();
    *data
        .get_mut("execution_args")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap() = json!(OTHER);
    *data
        .get_mut("system_inputs")
        .unwrap()
        .get_mut("workspace_id")
        .unwrap()
        .get_mut("resolved")
        .unwrap()
        .get_mut("value")
        .unwrap() = json!(OTHER);
    let tampered = ProtectedRecord::new(
        record.reference().record_id.clone(),
        record.reference().revision,
        changed_values,
    );
    assert!(
        BoundToolInput::restore(
            &tampered,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            snapshot.system_inputs.as_ref()
        )
        .is_err()
    );
    let mut changed_reference = snapshot.system_inputs.clone().unwrap();
    changed_reference.values_digest = canonical_digest(&json!({"workspace_id":OTHER}));
    assert!(
        BoundToolInput::restore(
            &record,
            &fixture.tool,
            &scope(),
            &id("run"),
            call,
            Some(&changed_reference)
        )
        .is_err()
    );
}

#[tokio::test]
async fn only_direct_optional_defaults_are_applied_not_conditional_or_required_model_defaults() {
    let mut required_default = descriptor();
    required_default.input_schema["required"] = json!(["query", "limit", "workspace_id"]);
    let fixture = Fixture::new(
        required_default,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    assert!(fixture.bind("call", None).await.is_err());

    let mut conditional = descriptor();
    conditional.agent_parameters.push("workspace_id".into());
    conditional.input_schema["properties"]["limit"]
        .as_object_mut()
        .unwrap()
        .remove("default");
    conditional.input_schema["if"] = json!({"properties":{"query":{"const":"strict"}}});
    conditional.input_schema["then"] = json!({"properties":{"limit":{"default":99}}});
    let fixture = Fixture::new(conditional, SystemInputRegistry::new(vec![]).unwrap(), None).await;
    let original = object(json!({"query":"strict","workspace_id":OWNED}));
    fixture.plan("call", original.clone()).await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.normalized_model_inputs(), &original);
}

#[tokio::test]
async fn an_explicit_nullable_model_value_is_not_replaced_by_its_default() {
    let mut tool = descriptor();
    tool.input_schema["properties"]["limit"]["type"] = json!(["integer", "null"]);
    let fixture = Fixture::new(
        tool,
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture
        .plan("call", object(json!({"query":"x","limit":null})))
        .await;
    let bound = fixture.bind("call", None).await.unwrap();
    assert_eq!(bound.input.original_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.normalized_model_inputs()["limit"], Value::Null);
    assert_eq!(bound.input.execution_args()["limit"], Value::Null);
}

#[tokio::test]
async fn zero_resolver_capacity_and_small_value_bounds_stop_before_unsafe_progress() {
    let fixture = Fixture::new(resolver_descriptor(), resolver_registry(), None).await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let resolver = Arc::new(Resolver::new(json!(OWNED)));
    let binder = fixture
        .binder(Some(resolver.clone()))
        .with_limits(InputBindingLimits {
            max_resolver_calls: 0,
            ..InputBindingLimits::default()
        })
        .unwrap();
    let budget = fixture.budget(fixture.store.clone()).await;
    assert!(
        binder
            .bind(&fixture.tool, &id("call"), &fixture.context, &budget)
            .await
            .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);

    let fixture = Fixture::new(
        descriptor(),
        registry(),
        Some(inputs(json!({"workspace_id":OWNED}))),
    )
    .await;
    fixture.plan("call", object(json!({"query":"x"}))).await;
    let binder = fixture
        .binder(None)
        .with_limits(InputBindingLimits {
            max_value_bytes: 4,
            ..InputBindingLimits::default()
        })
        .unwrap();
    assert!(
        binder
            .bind(
                &fixture.tool,
                &id("call"),
                &fixture.context,
                &fixture.budget(fixture.store.clone()).await
            )
            .await
            .is_err()
    );
    assert!(fixture.policy.executions.lock().unwrap().is_empty());
    assert!(
        fixture
            .store
            .load(&scope(), &id("run"))
            .await
            .unwrap()
            .snapshot
            .tool_ledger[0]
            .call
            .bound_input_ref
            .is_none()
    );
}
