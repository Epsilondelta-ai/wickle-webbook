//! Bounded response assembly and provider projection isolation.
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
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
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release-1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id(provider),
        target: JsonObject::from([("region".into(), json!("region-a"))]),
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

fn request() -> ModelRequest {
    ModelRequest {
        request_id: id("request"),
        purpose: ModelPurpose::Agent,
        route: route("first"),
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find the requested records".into(),
            }],
        }],
        tools: vec![ModelTool {
            name: id("search"),
            description: "Search available records".into(),
            model_input_schema: json!({
                "type":"object", "required":["query"], "additionalProperties":false,
                "properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}}
            }),
        }],
        output: ModelOutput::Text {},
        max_output_tokens: 128.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 32,
            max_tool_calls: 4,
        },
    }
}

fn text(value: &str) -> ModelEvent {
    ModelEvent::TextDelta { text: value.into() }
}

fn tool(index: u32, provider_id: Option<&str>, name: Option<&str>, delta: &str) -> ModelEvent {
    ModelEvent::ToolArgumentsDelta {
        index,
        provider_call_id: provider_id.map(str::to_owned),
        name: name.map(str::to_owned),
        delta: delta.into(),
    }
}

fn completed(finish: ModelFinish) -> ModelEvent {
    ModelEvent::ResponseCompleted {
        finish,
        metadata: ModelResponseMetadata::default(),
        continuation: vec![],
    }
}

async fn collect(
    request: &ModelRequest,
    events: Vec<ModelEvent>,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(request, Box::pin(stream::iter(events.into_iter().map(Ok)))).await
}

#[tokio::test]
async fn interleaved_tool_fragments_preserve_separate_arguments_and_unreported_metadata() {
    let request = request();
    let response = collect(
        &request,
        vec![
            text("Searching "),
            tool(0, Some("call-a"), Some("search"), r#"{"query":"ali"#),
            tool(1, Some("call-b"), Some("search"), r#"{"query":"\u03"#),
            text("records"),
            tool(0, None, None, r#"ce","limit":3}"#),
            tool(1, None, None, r#"bb"}"#),
            ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata {
                    provider_request_id: Some(id("provider-request")),
                    reported_model_id: None,
                    reported_model_version: None,
                    usage: Some(ModelUsage {
                        measurement: UsageMeasurement::Reported,
                        input_tokens: Some(12),
                        output_tokens: None,
                    }),
                },
                continuation: vec![],
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.route_digest, request.route.digest());
    assert_eq!(response.text, "Searching records");
    assert_eq!(response.tool_calls.len(), 2);
    assert_eq!(response.tool_calls[0].provider_call_id, id("call-a"));
    assert_eq!(
        response.tool_calls[0].model_inputs,
        JsonObject::from([("query".into(), json!("alice")), ("limit".into(), json!(3)),])
    );
    assert_eq!(response.tool_calls[1].provider_call_id, id("call-b"));
    assert_eq!(response.tool_calls[1].model_inputs["query"], json!("λ"));
    assert!(
        response
            .tool_calls
            .iter()
            .all(|call| call.validation == ToolCallValidation::Valid)
    );
    assert_eq!(response.metadata.reported_model_id, None);
    assert_eq!(response.metadata.reported_model_version, None);
    assert_eq!(response.metadata.usage.unwrap().output_tokens, None);
}

#[tokio::test]
async fn fragments_and_length_limited_responses_never_become_complete_tool_proposals() {
    for events in [
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"unfinished"#,
        )],
        vec![tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"complete"}"#),
            completed(ModelFinish::Length),
        ],
        vec![
            text("partial"),
            ModelEvent::ResponseError {
                kind: ModelFailureKind::Transport,
                metadata: ModelResponseMetadata::default(),
            },
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
    let disconnected = stream::iter(vec![
        Ok(tool(
            0,
            Some("call"),
            Some("search"),
            r#"{"query":"complete"}"#,
        )),
        Err(ContractError::new(
            ErrorCode::PersistenceUnavailable,
            "transport",
        )),
    ]);
    assert!(
        collect_model_response(&request(), Box::pin(disconnected))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn contradictory_or_repeated_terminals_and_events_after_completion_are_rejected() {
    for events in [
        vec![completed(ModelFinish::Stop), completed(ModelFinish::Stop)],
        vec![
            text("complete"),
            completed(ModelFinish::Stop),
            text("late fragment"),
        ],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Stop),
        ],
        vec![text("no call"), completed(ModelFinish::ToolCalls)],
        vec![
            tool(0, Some("call"), Some("search"), r#"{"query":"x"}"#),
            completed(ModelFinish::Refusal),
        ],
    ] {
        assert!(collect(&request(), events).await.is_err());
    }
}

#[tokio::test]
async fn malformed_or_ambiguous_argument_json_is_rejected_before_returning_calls() {
    for arguments in [
        r#"{"query":"first","query":"second"}"#,
        r#"{"query":"x","nested":{"key":1,"key":2}}"#,
        r#"{"query":"x","limit":NaN}"#,
        r#"{"query":"x"} trailing"#,
        "[]",
        "null",
        "true",
        "",
        r#"{"query":"unfinished"#,
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some("call"), Some("search"), arguments),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err(),
            "accepted arguments: {arguments}"
        );
    }
}

#[tokio::test]
async fn tool_schema_and_unknown_name_failures_remain_explicit_unexecutable_proposals() {
    let response = collect(
        &request(),
        vec![
            tool(0, Some("unknown"), Some("unregistered"), r#"{"query":"x"}"#),
            tool(1, Some("wrong-type"), Some("search"), r#"{"query":7}"#),
            tool(
                2,
                Some("hidden-input"),
                Some("search"),
                r#"{"query":"x","workspace_id":"invented"}"#,
            ),
            tool(
                3,
                Some("valid"),
                Some("search"),
                r#"{"query":"x","limit":2}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].validation,
        ToolCallValidation::UnknownTool
    );
    assert_eq!(
        response.tool_calls[1].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(
        response.tool_calls[2].validation,
        ToolCallValidation::InvalidArguments
    );
    assert_eq!(response.tool_calls[3].validation, ToolCallValidation::Valid);
    assert_eq!(
        response.tool_calls[2].model_inputs["workspace_id"],
        json!("invented")
    );
}

#[tokio::test]
async fn provider_call_collisions_and_metadata_reassignment_are_rejected() {
    for events in [
        vec![
            tool(0, Some("duplicate"), Some("search"), r#"{"query":"a"}"#),
            tool(1, Some("duplicate"), Some("search"), r#"{"query":"b"}"#),
        ],
        vec![
            tool(0, Some("original"), Some("search"), "{"),
            tool(0, Some("replacement"), None, r#""query":"a"}"#),
        ],
        vec![
            tool(0, Some("call"), Some("search"), "{"),
            tool(0, None, Some("other"), r#""query":"a"}"#),
        ],
        vec![tool(0, None, None, r#"{"query":"a"}"#)],
    ] {
        let mut events = events;
        events.push(completed(ModelFinish::ToolCalls));
        assert!(collect(&request(), events).await.is_err());
    }
    for (provider_id, name) in [
        ("", "search"),
        (" \n", "search"),
        ("call", ""),
        ("call", " \n"),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(provider_id), Some(name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ]
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn response_byte_delta_and_tool_count_limits_fail_without_truncating_to_success() {
    let mut limited = request();
    limited.limits.max_response_bytes = 3;
    limited.limits.max_delta_bytes = 2;
    assert!(
        collect(
            &limited,
            vec![text("é"), text("é"), completed(ModelFinish::Stop)]
        )
        .await
        .is_err()
    );
    limited = request();
    limited.limits.max_delta_bytes = 3;
    assert!(
        collect(&limited, vec![text("éé"), completed(ModelFinish::Stop)])
            .await
            .is_err()
    );
    limited = request();
    limited.limits.max_tool_calls = 1;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                tool(1, Some("second"), Some("search"), r#"{"query":"b"}"#),
                completed(ModelFinish::ToolCalls),
            ]
        )
        .await
        .is_err()
    );
    limited.limits.max_tool_calls = 0;
    assert!(
        collect(
            &limited,
            vec![
                tool(0, Some("first"), Some("search"), r#"{"query":"a"}"#),
                completed(ModelFinish::ToolCalls)
            ]
        )
        .await
        .is_err()
    );
    let response = collect(
        &limited,
        vec![text("text remains valid"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(response.text, "text remains valid");
}

#[tokio::test]
async fn empty_deltas_cannot_bypass_the_finite_event_limit() {
    let mut limited = request();
    limited.limits.max_events = 3;
    let at_limit = collect(
        &limited,
        vec![text("first"), text(" second"), completed(ModelFinish::Stop)],
    )
    .await
    .unwrap();
    assert_eq!(at_limit.text, "first second");
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let events = stream::repeat_with(|| Ok(text(""))).inspect(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
    });
    assert!(
        collect_model_response(&limited, Box::pin(events))
            .await
            .is_err()
    );
    assert_eq!(observed.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn wire_identifiers_use_the_protocol_rules_without_requiring_uuid_format() {
    for (provider_id, name) in [
        ("call\nname".to_owned(), "search".to_owned()),
        ("x".repeat(257), "search".to_owned()),
        ("call".to_owned(), "search tool".to_owned()),
        ("call".to_owned(), "search.tool".to_owned()),
        ("call".to_owned(), "검색".to_owned()),
        ("call".to_owned(), "x".repeat(65)),
    ] {
        assert!(
            collect(
                &request(),
                vec![
                    tool(0, Some(&provider_id), Some(&name), r#"{"query":"a"}"#),
                    completed(ModelFinish::ToolCalls),
                ],
            )
            .await
            .is_err()
        );
    }
    let response = collect(
        &request(),
        vec![
            tool(
                0,
                Some("provider:opaque/call"),
                Some("search"),
                r#"{"query":"a"}"#,
            ),
            completed(ModelFinish::ToolCalls),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        response.tool_calls[0].provider_call_id,
        id("provider:opaque/call")
    );
}

#[tokio::test]
async fn opaque_continuation_requires_the_exact_provider_route_and_release() {
    let mut original = request();
    let opaque =
        OpaqueContinuation::new(&original.route, json!({"thought_signature":"opaque-data"}));
    original.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Opaque {
            continuation: opaque.clone(),
        }],
    });
    assert!(
        collect(
            &original,
            vec![text("continued"), completed(ModelFinish::Stop)]
        )
        .await
        .is_ok()
    );
    let mut altered_routes = vec![route("second")];
    let mut changed = original.route.clone();
    changed.model_version = id("release-2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.api_contract.version = id("v2");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.connection_ref.version = id("rotated-connection");
    altered_routes.push(changed);
    let mut changed = original.route.clone();
    changed.target.insert("region".into(), json!("region-b"));
    altered_routes.push(changed);
    for changed in altered_routes {
        let mut incompatible = original.clone();
        incompatible.route = changed;
        assert!(
            collect(
                &incompatible,
                vec![text("must not continue"), completed(ModelFinish::Stop)]
            )
            .await
            .is_err()
        );
    }
    let foreign_opaque = OpaqueContinuation::new(&route("second"), json!({"signature":"foreign"}));
    assert!(
        collect(
            &request(),
            vec![
                text("answer"),
                ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![foreign_opaque],
                }
            ]
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn preflight_rejects_mismatched_port_bindings_and_oversized_input() {
    let request = request();
    let binding = ModelPortBinding {
        provider: request.route.provider.clone(),
        adapter: request.route.adapter.clone(),
        connection_ref: request.route.connection_ref.clone(),
    };
    assert!(binding.matches_route(&request.route));
    let mut wrong = binding.clone();
    wrong.provider = id("second");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding.clone();
    wrong.adapter.version = id("different-adapter");
    assert!(!wrong.matches_route(&request.route));
    wrong = binding;
    wrong.connection_ref.version = id("different-credential-binding");
    assert!(!wrong.matches_route(&request.route));
    let mut oversized = request;
    oversized.limits.max_input_bytes = 1;
    assert!(oversized.validate().is_err());
}

#[test]
fn host_options_affect_request_identity_and_bounds_without_changing_empty_request_encoding() {
    let mut request = request();
    let mut legacy = serde_json::to_value(&request).unwrap();
    legacy.as_object_mut().unwrap().remove("options");
    let restored: ModelRequest = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(restored.digest(), canonical_digest(&legacy));
    assert_eq!(restored, request);
    let original_digest = request.digest();
    request
        .options
        .insert("reasoning_effort".into(), json!("high"));
    request.validate().unwrap();
    assert_ne!(request.digest(), original_digest);
    let high_digest = request.digest();
    request
        .options
        .insert("reasoning_effort".into(), json!("low"));
    assert_ne!(request.digest(), high_digest);
    request.options.insert(
        "reasoning_effort".into(),
        json!("x".repeat(request.limits.max_input_bytes)),
    );
    assert_eq!(
        request.validate().unwrap_err().path,
        "model_request.input_size"
    );
}
