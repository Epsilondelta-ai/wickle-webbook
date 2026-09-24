use futures_util::stream;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
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

fn route(provider: &str) -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference(provider),
        catalog_revision: id("catalog-1"),
        routing_policy_revision: id("policy-1"),
        requested_model: id("example-model"),
        model_id: id("example-model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: reference(&format!("{provider}-adapter")),
        capability_revision: id("capabilities-1"),
        connection_ref: reference(&format!("{provider}-connection")),
    }
}

fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        request_id: id(&format!("{provider}-request")),
        purpose: ModelPurpose::Agent,
        route: route(provider),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Return the available information".into(),
            }],
        }],
        tools: vec![],
        output: ModelOutput::Text {},
        max_output_tokens: 32.try_into().unwrap(),
        options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        limits: ModelResponseLimits {
            max_input_bytes: 4096,
            max_response_bytes: 1024,
            max_delta_bytes: 256,
            max_events: 8,
            max_tool_calls: 0,
        },
    }
}

#[derive(Debug, PartialEq)]
struct ObservedCall {
    connection: VersionedRef,
    credential: &'static str,
    opaque_blocks: usize,
    options: JsonObject,
}

// These are synthetic Host-owned credentials. No real accounts or keys are used.
struct FirstModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}
struct SecondModel {
    observed: Arc<Mutex<Vec<ObservedCall>>>,
}

fn binding(provider: &str) -> ModelPortBinding {
    let route = route(provider);
    ModelPortBinding {
        provider: route.provider,
        adapter: route.adapter,
        connection_ref: route.connection_ref,
    }
}

fn observe(observed: &Mutex<Vec<ObservedCall>>, request: &ModelRequest, credential: &'static str) {
    observed.lock().unwrap().push(ObservedCall {
        connection: request.route.connection_ref.clone(),
        credential,
        options: request.options.clone(),
        opaque_blocks: request
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter(|content| matches!(content, ModelContent::Opaque { .. }))
            .count(),
    });
}

impl ModelPort for FirstModel {
    fn binding(&self) -> ModelPortBinding {
        binding("first")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-first-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "first response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![OpaqueContinuation::new(
                    &request.route,
                    json!({"signature":"first-only"}),
                )],
            }),
        ]))
    }
}

impl ModelPort for SecondModel {
    fn binding(&self) -> ModelPortBinding {
        binding("second")
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        observe(&self.observed, request, "synthetic-second-credential");
        Box::pin(stream::iter(vec![
            Ok(ModelEvent::TextDelta {
                text: "second ".into(),
            }),
            Ok(ModelEvent::TextDelta {
                text: "response".into(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::Stop,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ]))
    }
}

// Minimal Host dispatch demonstrating the public protocol. It does not implement
// routing policy, retries, budgets, or the agent's model/tool loop.
async fn invoke(
    registry: &BTreeMap<Id, Arc<dyn ModelPort>>,
    request: &ModelRequest,
) -> Result<ModelResponse, Box<dyn std::error::Error>> {
    request.validate()?;
    let port = registry
        .get(&request.route.provider)
        .ok_or_else(|| ContractError::new(ErrorCode::ComponentUnavailable, "provider"))?;
    if !port.binding().matches_route(&request.route) {
        return Err(ContractError::new(ErrorCode::InvalidReference, "connection_binding").into());
    }
    let context = ModelCallContext {
        attempt_id: id(&format!("{}-attempt", request.request_id)),
        run_id: id("run"),
        scope: Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        },
        cancellation: Default::default(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
    };
    Ok(collect_model_response(request, port.generate(request, &context)).await?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first_calls = Arc::new(Mutex::new(Vec::new()));
    let second_calls = Arc::new(Mutex::new(Vec::new()));
    let registry: BTreeMap<Id, Arc<dyn ModelPort>> = BTreeMap::from([
        (
            id("first"),
            Arc::new(FirstModel {
                observed: first_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
        (
            id("second"),
            Arc::new(SecondModel {
                observed: second_calls.clone(),
            }) as Arc<dyn ModelPort>,
        ),
    ]);
    let first = invoke(&registry, &request("first")).await?;
    assert_eq!(first.text, "first response");
    assert_eq!(first.continuation.len(), 1);
    assert_eq!(first.metadata.reported_model_version, None);

    let mut incompatible = request("second");
    incompatible.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: first.continuation[0].clone(),
        }],
    });
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());
    incompatible = request("second");
    incompatible.route.connection_ref = route("first").connection_ref;
    assert!(invoke(&registry, &incompatible).await.is_err());
    assert!(second_calls.lock().unwrap().is_empty());

    // A Host may build a new projection from ordinary conversation content when
    // it can preserve the required meaning. Foreign opaque data is not copied.
    let second = invoke(&registry, &request("second")).await?;
    assert_eq!(second.text, "second response");
    assert!(second.continuation.is_empty());
    assert_eq!(
        *first_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("first").connection_ref,
            credential: "synthetic-first-credential",
            opaque_blocks: 0,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        }]
    );
    assert_eq!(
        *second_calls.lock().unwrap(),
        vec![ObservedCall {
            connection: route("second").connection_ref,
            credential: "synthetic-second-credential",
            opaque_blocks: 0,
            options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        }]
    );
    println!(
        "model consumer: two concrete adapters through dyn ModelPort; one call per connection; foreign opaque and credential bindings rejected before dispatch"
    );
    Ok(())
}
