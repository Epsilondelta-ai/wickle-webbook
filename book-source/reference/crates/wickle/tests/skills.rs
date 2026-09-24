//! A real Agent loads instructions through its Tool ledger and projects committed bodies.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod support;
use futures_util::stream;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{completed, context, id, reference, request, scope};
use wickle::*;

struct Resolver {
    body: Mutex<String>,
    loads: AtomicUsize,
    uses: AtomicUsize,
    revoked: AtomicBool,
}
impl SkillResolver for Resolver {
    fn load<'a>(
        &'a self,
        _: &'a SkillRef,
        _: &'a SkillDefinition,
        _: &'a SkillCallContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(self.body.lock().unwrap().clone())
        })
    }
    fn authorize_use<'a>(
        &'a self,
        _: &'a LoadedSkill,
        _: &'a SkillCallContext,
    ) -> PortFuture<'a, ()> {
        Box::pin(async move {
            self.uses.fetch_add(1, Ordering::SeqCst);
            if self.revoked.load(Ordering::SeqCst) {
                Err(ContractError::new(
                    ErrorCode::AccessDenied,
                    "fixture.revoked",
                ))
            } else {
                Ok(())
            }
        })
    }
}
struct Catalog {
    skills: Arc<SkillRuntime>,
    base: Arc<support::Catalog>,
    self_grant: bool,
}
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        scope: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            if (reference.kind == ComponentKind::Hook && reference.id == id("audit"))
                || (reference.kind == ComponentKind::Tool && reference.id == id("ordinary"))
            {
                let mut metadata = self
                    .skills
                    .component_metadata(&ComponentRef {
                        kind: ComponentKind::Tool,
                        id: id("wickle.skills.load"),
                        version: Some(id("1")),
                    })
                    .unwrap();
                metadata.reference = reference.clone();
                metadata.model_name =
                    (reference.kind == ComponentKind::Tool).then(|| id("ordinary"));
                metadata.hook_position =
                    (reference.kind == ComponentKind::Hook).then_some(HookPosition::BeforeModel);
                metadata.capabilities.clear();
                metadata.manifest_digest = canonical_digest(&json!("audit"));
                return Ok(metadata);
            }
            if let Some(mut metadata) = self.skills.component_metadata(reference) {
                if self.self_grant && reference.kind == ComponentKind::Skill {
                    metadata.capabilities.insert(id("database_read"));
                }
                Ok(metadata)
            } else {
                self.base.resolve(reference, scope).await
            }
        })
    }
}
struct Model {
    binding: ModelPortBinding,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    tool_name: Mutex<String>,
    loads_before_final: AtomicUsize,
}
impl ModelPort for Model {
    fn binding(&self) -> ModelPortBinding {
        self.binding.clone()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let loads = self.loads_before_final.load(Ordering::SeqCst);
        let events = if call % (loads + 1) < loads {
            vec![
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some("load".into()),
                    name: Some(self.tool_name.lock().unwrap().clone()),
                    delta: json!({"skill_id":"multiply","version":"1"}).to_string(),
                },
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::ToolCalls,
                    metadata: Default::default(),
                    continuation: vec![],
                },
            ]
        } else {
            let factor = request
                .messages
                .iter()
                .flat_map(|m| &m.content)
                .filter_map(|content| match content {
                    ModelContent::Json { value }
                        if value["kind"] == "context_data" && value["origin"] == "skill" =>
                    {
                        value["content"][0]["text"]
                            .as_str()
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                            .and_then(|body| body["factor"].as_u64())
                    }
                    _ => None,
                })
                .next();
            vec![
                ModelEvent::TextDelta {
                    text: factor
                        .map(|factor| (factor * 6).to_string())
                        .unwrap_or_else(|| "unloaded".into()),
                },
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: Default::default(),
                    continuation: vec![],
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}
struct Fixture {
    base: support::Fixture,
    resolver: Arc<Resolver>,
    model: Arc<Model>,
    skills: Arc<SkillRuntime>,
    catalog: Arc<Catalog>,
    profile: AgentProfile,
    definition: SkillDefinition,
}
impl Fixture {
    fn new(self_grant: bool) -> Self {
        let base = support::Fixture::new(support::Response::Text, false);
        let body = json!({"factor":7}).to_string();
        let definition = SkillDefinition {
            skill: reference("multiply"),
            name: "Multiplication procedure".into(),
            description: "Load the registered calculation rule".into(),
            body_hash: SkillDefinition::hash_body(&body).unwrap(),
            body_bytes: body.len() as u64,
            assets: vec![],
            required_tool_capabilities: if self_grant {
                BTreeSet::from([id("database_read")])
            } else {
                BTreeSet::new()
            },
            config_schema: json!({"type":"object","additionalProperties":false}),
        };
        let resolver = Arc::new(Resolver {
            body: Mutex::new(body),
            loads: AtomicUsize::new(0),
            uses: AtomicUsize::new(0),
            revoked: AtomicBool::new(false),
        });
        let skills = Arc::new(
            SkillRuntime::new(
                SkillBindings {
                    scope: scope(),
                    state: base.store.clone(),
                    policy: base.bindings().policy,
                    resolver: resolver.clone(),
                    artifacts: None,
                },
                vec![definition.clone()],
                SkillRuntime::catalog_loader(),
                SkillLimits::default(),
            )
            .unwrap(),
        );
        let catalog = Arc::new(Catalog {
            skills: skills.clone(),
            base: base.catalog.clone(),
            self_grant,
        });
        let mut profile = support::profile();
        profile.tools.push(SkillRuntime::catalog_loader());
        profile.skills.push(SkillRef {
            skill_id: id("multiply"),
            version: id("1"),
            config: None,
        });
        profile.limits.max_tool_attempts = 2;
        let model = Arc::new(Model {
            binding: base.model.binding(),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
            tool_name: Mutex::new("skills_load".into()),
            loads_before_final: AtomicUsize::new(1),
        });
        Self {
            base,
            resolver,
            model,
            skills,
            catalog,
            profile,
            definition,
        }
    }
    fn bindings(&self) -> AgentBindings {
        let mut bindings = self.base.bindings();
        let mut router = support::Router::new();
        let mut catalog = router.snapshot.catalog().clone();
        catalog.models[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0]
            .capabilities
            .features
            .insert(id("tool_calling"));
        catalog.bindings[0].evidence[0].binding_digest = catalog.bindings[0]
            .contract_digest(&catalog.models[0])
            .unwrap();
        router.snapshot = RoutingSnapshot::new(catalog, router.snapshot.policy().clone()).unwrap();
        bindings.router = Arc::new(router);
        bindings.profile_resolver = self.catalog.clone();
        bindings.skills = Some(self.skills.clone());
        bindings.tools = Some(Arc::new(
            ToolRegistry::new(scope(), vec![self.skills.loader_tool()]).unwrap(),
        ));
        bindings.model_exchange = Arc::new(
            ModelExchange::new(self.model.clone(), bindings.policy.clone())
                .with_route_inspector(self.base.inspector.clone(), Duration::from_secs(1))
                .unwrap(),
        );
        bindings.settings.lease_ttl_ms = 30_000;
        bindings.settings.heartbeat_interval_ms = 5000;
        bindings
    }
    fn agent(&self) -> Agent {
        create_agent(self.profile.clone(), self.bindings()).unwrap()
    }
    async fn outcome(&self, handle: &RunHandle) -> RunOutcome {
        completed(
            // Let the engine's own deadline settle before the test watchdog.
            // This suite checks Skill semantics, not runner-specific CPU latency.
            tokio::time::timeout(
                Duration::from_millis(
                    self.profile
                        .limits
                        .max_elapsed_ms
                        .get()
                        .saturating_add(2000),
                ),
                handle.outcome(&context()),
            )
            .await
            .unwrap()
            .unwrap(),
        )
    }
}

#[tokio::test]
async fn a_skill_is_loaded_through_the_tool_ledger_then_used_from_its_committed_body() {
    let f = Fixture::new(false);
    let agent = f.agent();
    let handle = completed(agent.start(request("first"), context()).await.unwrap());
    let outcome = f.outcome(&handle).await;
    assert_eq!(outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(
        outcome.output,
        vec![InputContent::Text { text: "42".into() }]
    );
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("settled loader")
    };
    let record = f
        .base
        .store
        .read_record(&scope(), result.skill_ref.as_ref().unwrap())
        .await
        .unwrap();
    let loaded: LoadedSkill = serde_json::from_value(record.value().clone()).unwrap();
    assert_eq!(loaded.body(), f.resolver.body.lock().unwrap().as_str());
    assert_eq!(
        result.content[0],
        InputContent::Json {
            value: json!({"kind":"loaded_skill","skill":{"skill_id":"multiply","version":"1"},"definition_digest":loaded.definition_digest()})
        }
    );
    let image = f.base.store.export_checkpoint(&scope()).unwrap();
    let restored = StateStoreCheckpoint::from_json(
        &serde_json::to_string(&image).unwrap(),
        &scope(),
        &image.digest(),
    )
    .unwrap();
    assert_eq!(
        MemoryStateStore::from_checkpoint(restored)
            .load(&scope(), handle.run_id())
            .await
            .unwrap(),
        saved
    );
    let replay = completed(agent.start(request("first"), context()).await.unwrap());
    assert_eq!(f.outcome(&replay).await, outcome);
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_skill_cannot_supply_its_own_required_tool_capability() {
    let f = Fixture::new(true);
    ProfileValidator::new(f.catalog.as_ref())
        .validate(&f.profile, &scope())
        .await
        .unwrap();
    let error = f
        .agent()
        .start(request("first"), context())
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CapabilityUnsupported);
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 0);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_changed_or_truncated_body_does_not_become_loaded_instructions() {
    for body in [
        "{\"factor\":8}".to_owned(),
        "{\"factor\":".to_owned(),
        "x".repeat(40 * 1024),
    ] {
        let f = Fixture::new(false);
        *f.resolver.body.lock().unwrap() = body;
        let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
        f.outcome(&handle).await;
        let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
            panic!("settled failed loader")
        };
        assert_eq!(result.status, ToolResultStatus::Failed);
        assert!(result.skill_ref.is_none());
        assert_eq!(f.resolver.uses.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn revoked_skill_access_stops_before_the_next_model_even_with_a_saved_body() {
    let f = Fixture::new(false);
    f.resolver.revoked.store(true, Ordering::SeqCst);
    let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
    assert_eq!(f.outcome(&handle).await.result.status(), RunStatus::Failed);
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 1);
}

struct Audit(Mutex<Vec<Vec<ContextOrigin>>>);
struct CompactHistory(AtomicUsize);
impl HostContextCompactor for CompactHistory {
    fn compact<'a>(
        &'a self,
        _: &'a CompactionRequest,
        _: &'a ContextStrategyContext,
    ) -> PortFuture<'a, String> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(
                "The calculation Skill was loaded. Continue using its registered instructions."
                    .into(),
            )
        })
    }
}
#[tokio::test]
async fn compaction_keeps_loaded_skill_instructions_after_loader_rounds_are_summarized() {
    let mut f = Fixture::new(false);
    f.model.loads_before_final.store(6, Ordering::SeqCst);
    f.profile.limits.max_model_calls = 8.try_into().unwrap();
    f.profile.limits.max_tool_attempts = 6;
    let mut bindings = f.bindings();
    bindings.settings.projection_limits.max_bytes = 4000;
    let summary = Arc::new(CompactHistory(AtomicUsize::new(0)));
    bindings.context_runtime = Some(Arc::new(
        ContextRuntime::new(
            scope(),
            Arc::new(BoundedContextStrategy),
            Some(ContextCompactor::Host {
                definition: reference("summary"),
                compressor: summary.clone(),
            }),
            ContextRewriteLimits::default(),
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = completed(agent.start(request("first"), context()).await.unwrap());
    assert_eq!(
        f.outcome(&handle).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 1);
    assert!(summary.0.load(Ordering::SeqCst) > 0);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert!(saved.snapshot.context_revision_ref.is_some());
    assert!(saved.snapshot.tool_ledger.iter().all(
        |entry| matches!(&entry.state,ToolCallState::Settled {result} if result.skill_ref.is_some())
    ));
    let checkpoint = f.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&checkpoint).unwrap(),
        &scope(),
        &checkpoint.digest(),
    )
    .unwrap();
}
#[tokio::test]
async fn a_large_skill_configuration_cannot_bypass_the_loader_output_bound() {
    let mut f = Fixture::new(false);
    f.definition.config_schema = json!({"type":"object","additionalProperties":true});
    f.profile.skills[0].config = Some(std::collections::BTreeMap::from([(
        "fixture".into(),
        json!("x".repeat(70_000)),
    )]));
    let limits = SkillLimits {
        max_body_bytes: f.definition.body_bytes,
        max_total_body_bytes: f.definition.body_bytes,
        ..Default::default()
    };
    f.skills = Arc::new(
        SkillRuntime::new(
            SkillBindings {
                scope: scope(),
                state: f.base.store.clone(),
                policy: f.base.bindings().policy,
                resolver: f.resolver.clone(),
                artifacts: None,
            },
            vec![f.definition.clone()],
            SkillRuntime::catalog_loader(),
            limits,
        )
        .unwrap(),
    );
    f.catalog = Arc::new(Catalog {
        skills: f.skills.clone(),
        base: f.base.catalog.clone(),
        self_grant: false,
    });
    let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
    f.outcome(&handle).await;
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("settled loader")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert!(result.skill_ref.is_none());
}
struct AssetPolicy(bool);
impl PolicyPort for AssetPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            Ok(
                if self.0 && matches!(request.action, PolicyAction::ReadArtifact { .. }) {
                    PolicyDecision::Deny {
                        reason: id("asset_denied"),
                    }
                } else {
                    PolicyDecision::Allow {}
                },
            )
        })
    }
}
#[tokio::test]
async fn supporting_assets_require_current_access_before_the_instruction_body_is_loaded() {
    for denied in [false, true] {
        let mut f = Fixture::new(false);
        let policy = Arc::new(
            PolicyGate::new(Arc::new(AssetPolicy(denied)), Duration::from_secs(1)).unwrap(),
        );
        let artifacts = Arc::new(
            ArtifactRuntime::new(
                Arc::new(MemoryArtifactStore::default()),
                policy,
                f.base.ids.clone(),
                ArtifactLimits::default(),
            )
            .unwrap(),
        );
        let asset = artifacts
            .put(
                ArtifactInput {
                    media_type: id("text/plain"),
                    bytes: b"supporting asset".to_vec(),
                    source: None,
                },
                &context(),
                None,
            )
            .await
            .unwrap();
        f.definition.assets.push(asset.reference);
        f.skills = Arc::new(
            SkillRuntime::new(
                SkillBindings {
                    scope: scope(),
                    state: f.base.store.clone(),
                    policy: f.base.bindings().policy,
                    resolver: f.resolver.clone(),
                    artifacts: Some(artifacts),
                },
                vec![f.definition.clone()],
                SkillRuntime::catalog_loader(),
                SkillLimits::default(),
            )
            .unwrap(),
        );
        f.catalog = Arc::new(Catalog {
            skills: f.skills.clone(),
            base: f.base.catalog.clone(),
            self_grant: false,
        });
        let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
        let outcome = f.outcome(&handle).await;
        let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
        let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
            panic!("settled loader")
        };
        assert_eq!(
            result.status,
            if denied {
                ToolResultStatus::Failed
            } else {
                ToolResultStatus::Succeeded
            }
        );
        assert_eq!(
            f.resolver.loads.load(Ordering::SeqCst),
            usize::from(!denied)
        );
        assert_eq!(
            outcome.output,
            vec![InputContent::Text {
                text: if denied { "unloaded" } else { "42" }.into()
            }]
        );
    }
}
fn replace_reference(value: &mut Value, old: &Value, new: &Value) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        Value::Array(values) => {
            for value in values {
                replace_reference(value, old, new)
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                replace_reference(value, old, new)
            }
        }
        _ => {}
    }
}
fn rehash_records(image: &mut Value) {
    let limit = image["records"].as_array().unwrap().len() * 2;
    for _ in 0..limit {
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
        let Some((old, new)) = changed else { return };
        replace_reference(image, &old, &new);
    }
    panic!("record graph did not converge");
}
#[tokio::test]
async fn self_consistent_record_hashes_cannot_replace_the_pinned_skill_body() {
    let f = Fixture::new(false);
    let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
    f.outcome(&handle).await;
    let mut image =
        serde_json::to_value(f.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    let record = image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| record["value"]["schema_version"] == "wickle.loaded-skill.v1")
        .unwrap();
    record["value"]["body"] = json!("{\"factor\":8}");
    rehash_records(&mut image);
    for record in image["records"].as_array().unwrap() {
        assert_eq!(
            record["reference"]["digest"],
            serde_json::to_value(canonical_digest(&record["value"])).unwrap()
        );
    }
    let error =
        StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(&image))
            .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidSkill);
    assert_eq!(
        f.outcome(&handle).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
}
struct ForgedSkill {
    definition: SkillDefinition,
    body: String,
}
impl ToolExecutor for ForgedSkill {
    fn execute<'a>(
        &'a self,
        _: &'a JsonObject,
        context: &'a ToolExecutionContext,
    ) -> PortFuture<'a, ToolExecutionResult> {
        Box::pin(async move {
            let loaded:LoadedSkill=serde_json::from_value(json!({"schema_version":"wickle.loaded-skill.v1","scope":context.scope,"run_id":context.run_id,"call_id":context.call_id,"selection":{"skill_id":"multiply","version":"1"},"definition_digest":self.definition.digest(),"body":self.body,"assets":[]})).unwrap();
            Ok(ToolExecutionResult {
                outcome: ToolExecutionOutcome::LoadedSkill {
                    loaded: Box::new(loaded),
                },
                effect: ToolEffect::NotApplied,
                receipt: None,
            })
        })
    }
}
#[tokio::test]
async fn ordinary_tools_cannot_claim_skill_origin_even_with_a_correct_registered_body() {
    let mut f = Fixture::new(false);
    *f.model.tool_name.lock().unwrap() = "ordinary".into();
    let mut descriptor = f.skills.loader_descriptor().clone();
    descriptor.tool = reference("ordinary");
    descriptor.name = id("ordinary");
    let compiled = SchemaCompiler::new()
        .compile(descriptor, &SystemInputRegistry::new(vec![]).unwrap())
        .unwrap();
    f.profile
        .tools
        .push(ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("ordinary"),
            version: id("1"),
            bindings: None,
            config: None,
        }));
    let mut bindings = f.bindings();
    bindings.tools = Some(Arc::new(
        ToolRegistry::new(
            scope(),
            vec![
                f.skills.loader_tool(),
                ToolRegistration {
                    compiled,
                    executor: Arc::new(ForgedSkill {
                        definition: f.definition.clone(),
                        body: f.resolver.body.lock().unwrap().clone(),
                    }),
                },
            ],
        )
        .unwrap(),
    ));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = completed(agent.start(request("first"), context()).await.unwrap());
    f.outcome(&handle).await;
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    let ToolCallState::Settled { result } = &saved.snapshot.tool_ledger[0].state else {
        panic!("settled ordinary tool")
    };
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert!(result.skill_ref.is_none());
    assert!(result.content.is_empty());
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 0);
    assert_eq!(f.resolver.uses.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn repeated_load_calls_reuse_one_body_and_do_not_double_charge_the_total_body_limit() {
    let mut f = Fixture::new(false);
    f.model.loads_before_final.store(2, Ordering::SeqCst);
    let limits = SkillLimits {
        max_body_bytes: f.definition.body_bytes,
        max_total_body_bytes: f.definition.body_bytes,
        ..Default::default()
    };
    f.skills = Arc::new(
        SkillRuntime::new(
            SkillBindings {
                scope: scope(),
                state: f.base.store.clone(),
                policy: f.base.bindings().policy,
                resolver: f.resolver.clone(),
                artifacts: None,
            },
            vec![f.definition.clone()],
            SkillRuntime::catalog_loader(),
            limits,
        )
        .unwrap(),
    );
    f.catalog = Arc::new(Catalog {
        skills: f.skills.clone(),
        base: f.base.catalog.clone(),
        self_grant: false,
    });
    let handle = completed(f.agent().start(request("first"), context()).await.unwrap());
    assert_eq!(
        f.outcome(&handle).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 1);
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 3);
    let saved = f.base.store.load(&scope(), handle.run_id()).await.unwrap();
    assert_eq!(saved.snapshot.tool_ledger.len(), 2);
    assert!(saved.snapshot.tool_ledger.iter().all(|entry|matches!(&entry.state,ToolCallState::Settled {result} if result.status==ToolResultStatus::Succeeded&&result.skill_ref.is_some())));
}
impl HookHandler for Audit {
    fn call<'a>(&'a self, input: &'a HookInput, _: &'a HookContext) -> PortFuture<'a, HookOutput> {
        Box::pin(async move {
            let HookInput::BeforeModel { context_items, .. } = input else {
                panic!("before-model only")
            };
            self.0
                .lock()
                .unwrap()
                .push(context_items.iter().map(|item| item.origin).collect());
            Ok(HookOutput::Context { additions: vec![] })
        })
    }
}
#[tokio::test]
async fn hook_history_uses_only_skills_loaded_before_its_original_model_step() {
    let mut f = Fixture::new(false);
    let audit = Arc::new(Audit(Mutex::new(vec![])));
    let definition = HookDefinition {
        hook: reference("audit"),
        position: HookPosition::BeforeModel,
        priority: 0,
        required: true,
        timeout_ms: 1000,
        max_output_bytes: 4096,
    };
    f.profile.hooks = Some(vec![HookRef::Catalog(CatalogHookRef {
        hook_id: id("audit"),
        version: id("1"),
        position: HookPosition::BeforeModel,
    })]);
    let mut bindings = f.bindings();
    bindings.hooks = Some(Arc::new(HookRuntime::new(
        bindings.state.clone(),
        bindings.policy.clone(),
        bindings.clock.clone(),
        bindings.ids.clone(),
        Arc::new(
            HookRegistry::new(
                scope(),
                vec![HookRegistration {
                    definition,
                    handler: audit.clone(),
                }],
            )
            .unwrap(),
        ),
    )));
    let agent = create_agent(f.profile.clone(), bindings).unwrap();
    let handle = completed(agent.start(request("first"), context()).await.unwrap());
    assert_eq!(
        f.outcome(&handle).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
    assert_eq!(
        *audit.0.lock().unwrap(),
        vec![vec![], vec![ContextOrigin::Skill]]
    );
    let image = f.base.store.export_checkpoint(&scope()).unwrap();
    StateStoreCheckpoint::from_json(
        &serde_json::to_string(&image).unwrap(),
        &scope(),
        &image.digest(),
    )
    .unwrap();
}

#[tokio::test]
async fn new_runs_keep_the_listing_and_cannot_replace_the_same_skill_version_in_a_session() {
    let mut f = Fixture::new(false);
    let agent = f.agent();
    let first = completed(agent.start(request("first"), context()).await.unwrap());
    f.outcome(&first).await;
    let session = f
        .base
        .store
        .load_session(&scope(), &id("session"))
        .await
        .unwrap();
    let second = completed(agent.start(request("second"), context()).await.unwrap());
    assert_eq!(
        f.outcome(&second).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
    assert_eq!(
        f.base
            .store
            .load_session(&scope(), &id("session"))
            .await
            .unwrap()
            .prompt_snapshot,
        session.prompt_snapshot
    );
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 2);
    let saved = f.base.store.load(&scope(), second.run_id()).await.unwrap();
    let record = f
        .base
        .store
        .read_record(&scope(), saved.snapshot.skill_plan_ref.as_ref().unwrap())
        .await
        .unwrap();
    let plan = SkillPlan::restore(&record, &saved.snapshot.profile).unwrap();
    let mut definition = plan.skills()[0].definition.clone();
    let changed = json!({"factor":8}).to_string();
    definition.body_hash = SkillDefinition::hash_body(&changed).unwrap();
    definition.body_bytes = changed.len() as u64;
    *f.resolver.body.lock().unwrap() = changed;
    f.skills = Arc::new(
        SkillRuntime::new(
            SkillBindings {
                scope: scope(),
                state: f.base.store.clone(),
                policy: f.base.bindings().policy,
                resolver: f.resolver.clone(),
                artifacts: None,
            },
            vec![definition],
            SkillRuntime::catalog_loader(),
            SkillLimits::default(),
        )
        .unwrap(),
    );
    f.catalog = Arc::new(Catalog {
        skills: f.skills.clone(),
        base: f.base.catalog.clone(),
        self_grant: false,
    });
    assert!(f.agent().start(request("third"), context()).await.is_err());
    assert_eq!(f.model.calls.load(Ordering::SeqCst), 4);
    assert_eq!(f.resolver.loads.load(Ordering::SeqCst), 2);
    assert_eq!(
        f.outcome(&first).await.output,
        vec![InputContent::Text { text: "42".into() }]
    );
}
