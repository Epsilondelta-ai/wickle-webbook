// Real SQLite and scoped artifact/Skill loading with synthetic model and inspector.
// This consumer makes no provider network calls and does not test a production data service.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;

use wickle_model_router::PolicyModelRouter;
use wickle_adapter_runtime::{AdapterRegistry,AdapterRuntime,CatalogToolRegistration};
use wickle_state_sqlite::SqliteStateStore;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

struct Inspector;
impl ModelRouteInspector for Inspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        // Fixture echo only: production inspectors must fetch authoritative metadata.
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-inspection"),
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ContractError> {
        // Deliberately synthetic estimate for this fixed fixture, not a tokenizer.
        Ok(512)
    }
}

fn routing(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let capabilities = ModelCapabilities {
        revision: id("features"),
        features: BTreeSet::from([id("text"), id("tool_calling")]),
        options_schema: json!({"type":"object","additionalProperties":false}),
        context_window: 4096.try_into().unwrap(),
        max_output_tokens: 512.try_into().unwrap(),
    };
    let model = ModelDefinition {
        model_key: id("synthetic"),
        family: id("synthetic"),
        provider: id("synthetic"),
        model_id: id("fixture-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: capabilities.clone(),
        evidence: vec![],
    };
    let mut binding = ModelBinding {
        binding: reference("primary"),
        model: model.reference(),
        requested_model: model.model_id.clone(),
        adapter: reference("synthetic-adapter"),
        connection_ref: reference("synthetic-connection"),
        target: JsonObject::new(),
        target_schema: json!({"type":"object","additionalProperties":false}),
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        deployment_revision: None,
        version_semantics: VersionSemantics::Pinned,
        capabilities,
        support: ModelSupportStatus::ContractTested,
        evidence: vec![],
    };
    binding.evidence.push(ModelValidationEvidence {
        kind: ModelValidationKind::ContractTest,
        binding_digest: binding.contract_digest(&model)?,
        checked_at_ms: 1000,
        evidence_ref: id("synthetic-contract-test"),
        passed: true,
    });
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog-1"),
            scope: scope.clone(),
            models: vec![model],
            bindings: vec![binding],
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy-1"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("primary"),
                fallbacks: vec![],
                fallback_on: vec![],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        request: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(request.version.clone().unwrap_or_else(|| id("1"))),
                    ..request.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(request.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
struct Policy;
impl PolicyPort for Policy {
    fn authorize<'a>(
        &'a self,
        _: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async { Ok(PolicyDecision::Allow {}) })
    }
}
struct Resolver {artifact:ArtifactRef,artifacts:Arc<ArtifactRuntime>,loads:AtomicUsize,revoked:AtomicBool}
impl SkillResolver for Resolver {
 fn load<'a>(&'a self,_:&'a SkillRef,_:&'a SkillDefinition,context:&'a SkillCallContext)->PortFuture<'a,String>{Box::pin(async move {
  self.loads.fetch_add(1,Ordering::SeqCst);
  let bytes=self.artifacts.get(&self.artifact,&execution(context),Some(context.deadline)).await?.bytes;
  String::from_utf8(bytes).map_err(|_|ContractError::new(ErrorCode::InvalidSkill,"consumer.utf8"))
 })}
 fn authorize_use<'a>(&'a self,_:&'a LoadedSkill,context:&'a SkillCallContext)->PortFuture<'a,()>{Box::pin(async move {
  if self.revoked.load(Ordering::SeqCst){return Err(ContractError::new(ErrorCode::AccessDenied,"consumer.revoked"));}
  self.artifacts.stat(&self.artifact,&execution(context),Some(context.deadline)).await?;Ok(())
 })}
}
fn execution(context:&SkillCallContext)->ExecutionContext {
 ExecutionContext::new(ExecutionContextData {scope:context.scope.clone(),principal_ref:context.principal_ref.clone(),capability_grant_ref:context.capability_grant_ref.clone(),trace_context:None,system_inputs:None},context.cancellation.clone())
}
struct Metadata(Arc<SkillRuntime>);
impl ProfileResolver for Metadata {
 fn resolve<'a>(&'a self,reference:&'a ComponentRef,scope:&'a Scope)->PortFuture<'a,ComponentMetadata>{Box::pin(async move {
  match self.0.component_metadata(reference){Some(metadata)=>Ok(metadata),None=>Catalog.resolve(reference,scope).await}
 })}
}
struct Model(AtomicUsize);
impl ModelPort for Model {
 fn binding(&self)->ModelPortBinding {ModelPortBinding {provider:id("synthetic"),adapter:reference("synthetic-adapter"),connection_ref:reference("synthetic-connection")}}
 fn generate<'a>(&'a self,request:&'a ModelRequest,_:&'a ModelCallContext)->PortStream<'a,ModelEvent>{
  let index=self.0.fetch_add(1,Ordering::SeqCst);
  let events=if index==0 {vec![ModelEvent::ToolArgumentsDelta {index:0,provider_call_id:Some("load".into()),name:Some("skills_load".into()),delta:json!({"skill_id":"calculation","version":"1"}).to_string()},ModelEvent::ResponseCompleted {finish:ModelFinish::ToolCalls,metadata:Default::default(),continuation:vec![]}]}else{
   // Deterministic port reads the actual projected Skill data; no LLM behavior is claimed.
   let factor=request.messages.iter().flat_map(|m|&m.content).find_map(|content|match content {ModelContent::Json{value} if value["kind"]=="context_data"&&value["origin"]=="skill"=>value["content"][0]["text"].as_str().and_then(|s|serde_json::from_str::<serde_json::Value>(s).ok()).and_then(|body|body["factor"].as_u64()),_=>None}).expect("complete loaded Skill in projection");
   vec![ModelEvent::TextDelta {text:(factor*6).to_string()},ModelEvent::ResponseCompleted {finish:ModelFinish::Stop,metadata:Default::default(),continuation:vec![]}]
  };
  Box::pin(stream::iter(events.into_iter().map(Ok)))
 }
}
fn make_agent(profile:AgentProfile,context:&ExecutionContext,store:Arc<SqliteStateStore>,skills:Arc<SkillRuntime>,artifacts:Arc<ArtifactRuntime>,model:Arc<Model>,policy:Arc<PolicyGate>)->Result<Agent,ContractError>{
 let clock=Arc::new(SystemClock::new());
 let loader=skills.loader_tool();
 let metadata=skills.component_metadata(&ComponentRef {kind:ComponentKind::Tool,id:loader.compiled.descriptor().tool.id.clone(),version:Some(loader.compiled.descriptor().tool.version.clone())}).ok_or_else(||ContractError::new(ErrorCode::ComponentUnavailable,"consumer.loader"))?;
 let registry=Arc::new(AdapterRegistry::new(context.data.scope.clone(),vec![],vec![],vec![CatalogToolRegistration {metadata,tool:loader}],vec![],vec![])?);
 let runtime=Arc::new(AdapterRuntime::new(registry,store.clone(),policy.clone(),clock.clone()));
 create_agent(profile,AgentBindings {scope:context.data.scope.clone(),state:store,policy:policy.clone(),profile_resolver:Arc::new(Metadata(skills.clone())),model_exchange:Arc::new(ModelExchange::new(model,policy).with_route_inspector(Arc::new(Inspector),Duration::from_secs(1))?),router:Arc::new(PolicyModelRouter::new(routing(&context.data.scope)?)?),host_instructions:vec!["Use the explicitly selected procedure.".into()],system_inputs:SystemInputRegistry::new(vec![])?,tools:None,system_input_resolver:None,external_receipt_verifier:None,hooks:None,components:Some(runtime),context_sources:None,context_token_estimator:None,context_runtime:None, verification: None,skills:Some(skills),artifacts:Some(artifacts),clock,ids:Arc::new(RandomIdSource),token_estimator:Arc::new(Estimate),settings:AgentSettings {require_durable:true,max_output_tokens:128.try_into().unwrap(),..Default::default()}})
}
#[tokio::main(flavor="current_thread")]
async fn main()->Result<(),Box<dyn std::error::Error>> {
 let scope=Scope {tenant_id:id("example"),workspace_id:id("workspace"),user_id:None};
 let context=ExecutionContext::new(ExecutionContextData {scope:scope.clone(),principal_ref:id("reader"),capability_grant_ref:id("grant"),trace_context:None,system_inputs:None},Default::default());
 let policy=Arc::new(PolicyGate::new(Arc::new(Policy),Duration::from_secs(1))?);
 let artifacts=Arc::new(ArtifactRuntime::new(Arc::new(MemoryArtifactStore::default()),policy.clone(),Arc::new(RandomIdSource),ArtifactLimits::default())?);
 let body=json!({"factor":7}).to_string();
 let metadata=artifacts.put(ArtifactInput {media_type:id("text/plain"),bytes:body.as_bytes().to_vec(),source:Some(reference("calculation"))},&context,None).await?;
 let evidence=artifacts.evidence(&metadata.reference,id("body"),Some(body.clone()),&context,None).await?;
 assert_eq!(evidence.version,id("1"));
 let resolver=Arc::new(Resolver {artifact:metadata.reference.clone(),artifacts:artifacts.clone(),loads:AtomicUsize::new(0),revoked:AtomicBool::new(false)});
 let definition=SkillDefinition {skill:reference("calculation"),name:"Calculation".into(),description:"Load the exact calculation procedure".into(),body_hash:SkillDefinition::hash_body(&body)?,body_bytes:body.len() as u64,assets:vec![],required_tool_capabilities:Default::default(),config_schema:json!({"type":"object","additionalProperties":false})};
 let path=std::env::temp_dir().join(format!("wickle-skills-consumer-{}.sqlite3",RandomIdSource.next_id()?));
 let store=Arc::new(SqliteStateStore::open(&path)?);
 let make_skills=|store:Arc<SqliteStateStore>|SkillRuntime::new(SkillBindings {scope:scope.clone(),state:store,policy:policy.clone(),resolver:resolver.clone(),artifacts:Some(artifacts.clone())},vec![definition.clone()],SkillRuntime::catalog_loader(),SkillLimits::default());
 let skills=Arc::new(make_skills(store.clone())?);
 let mut profile=AgentProfile::from_json(r#"{"schema_version":"wickle.agent-profile.v1","agent_id":"example","version":"1","name":"Skill example","description":"A scoped instruction loader","instructions":{"text":"Use the registered calculation procedure."},"model_binding":"primary","tools":[],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},"limits":{"max_model_calls":2,"max_tool_attempts":1,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":30000}}"#)?;
 profile.tools.push(SkillRuntime::catalog_loader());profile.skills.push(SkillRef {skill_id:id("calculation"),version:id("1"),config:None});
 let model=Arc::new(Model(AtomicUsize::new(0)));
 let agent=make_agent(profile.clone(),&context,store.clone(),skills.clone(),artifacts.clone(),model.clone(),policy.clone())?;
 let request=RunRequest {request_id:id("request"),session_id:id("session"),input:vec![InputContent::Text {text:"Apply the calculation procedure to 6.".into()}],trigger:RunTrigger::User{},model_options:Default::default(),output_contract:None};
 let handle=completed(agent.start(request.clone(),context.clone()).await?)?;
 let outcome=completed(handle.outcome(&context).await?)?;
 assert_eq!(outcome.output,vec![InputContent::Text{text:"42".into()}]);assert_eq!(resolver.loads.load(Ordering::SeqCst),1);assert_eq!(model.0.load(Ordering::SeqCst),2);
 let events:Vec<_>=handle.events(0,context.clone()).try_collect().await?;assert_eq!(events.last().unwrap().event_type,"run.finished");
 let saved=store.load(&scope,handle.run_id()).await?;
 let ToolCallState::Settled {result}=&saved.snapshot.tool_ledger[0].state else {panic!("settled loader")};
 let reference=result.skill_ref.as_ref().expect("protected complete body").clone();
 let record=store.read_record(&scope,&reference).await?;
 let loaded:LoadedSkill=serde_json::from_value(record.value().clone())?;assert_eq!(loaded.body(),body);
 drop(agent);drop(skills);drop(store);
 let reopened=Arc::new(SqliteStateStore::open(&path)?);assert_eq!(reopened.load(&scope,handle.run_id()).await?,saved);assert_eq!(reopened.read_record(&scope,&reference).await?,record);
 let skills=Arc::new(make_skills(reopened.clone())?);
 let restored=make_agent(profile,&context,reopened,skills.clone(),artifacts.clone(),model.clone(),policy)?;
 let replay=completed(restored.start(request,context.clone()).await?)?;assert_eq!(completed(replay.outcome(&context).await?)?,outcome);assert_eq!(model.0.load(Ordering::SeqCst),2);assert_eq!(resolver.loads.load(Ordering::SeqCst),1);
 resolver.revoked.store(true,Ordering::SeqCst);assert_eq!(skills.context_items(&saved.snapshot,&context,None,tokio::time::Instant::now()+Duration::from_secs(1)).await.unwrap_err().code,ErrorCode::AccessDenied);
 let mut foreign=context.clone();foreign.data.scope.workspace_id=id("another-workspace");assert_eq!(artifacts.get(&metadata.reference,&foreign,None).await.unwrap_err().code,ErrorCode::AccessDenied);
 println!("skills consumer: artifact-backed complete instructions; exact version and evidence; real SQLite body/result persistence; independent reopen and replay without new calls; current Skill and artifact scope checks (synthetic model, no network)");
 Ok(())
}
