use crate::{
    BedrockAudience, BedrockConnection, BedrockCredentialContext, BedrockCredentialProvider,
    BedrockSelector, auth, error,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// Provider-documented release facts, independent of inference-profile mutability.
#[derive(Clone, Debug)]
pub struct BedrockSnapshot {
    /// Exact foundation model ID reported by AWS metadata.
    pub model_id: Id,
    /// Release established by trusted provider evidence.
    pub model_version: Id,
    /// Host-owned evidence reference.
    pub evidence_ref: Id,
}
/// Queries AWS control-plane metadata with separately supplied IAM credentials.
#[derive(Clone)]
pub struct BedrockInspector {
    connection: BedrockConnection,
    credentials: Arc<dyn BedrockCredentialProvider>,
    snapshots: Arc<BTreeMap<Id, BedrockSnapshot>>,
}
impl BedrockInspector {
    /// Construct without network calls. A Bedrock bearer token cannot authorize this lookup.
    pub fn new(
        connection: BedrockConnection,
        credentials: Arc<dyn BedrockCredentialProvider>,
        snapshots: Vec<BedrockSnapshot>,
    ) -> Result<Self, ContractError> {
        let mut known = BTreeMap::new();
        for snapshot in snapshots {
            if known.insert(snapshot.model_id.clone(), snapshot).is_some() {
                return Err(error(ErrorCode::InvalidConfiguration, "snapshots"));
            }
        }
        Ok(Self {
            connection,
            credentials,
            snapshots: Arc::new(known),
        })
    }
}
impl ModelRouteInspector for BedrockInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let options = &self.connection.0.options;
            let profile = matches!(options.selector, BedrockSelector::InferenceProfile(_));
            let mut url = self.connection.0.metadata_base.clone();
            url.path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "metadata_url"))?
                .clear()
                .push(if profile {
                    "inference-profiles"
                } else {
                    "foundation-models"
                })
                .push(options.selector.value());
            let operation = async {
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "accept",
                    reqwest::header::HeaderValue::from_static("application/json"),
                );
                let headers = auth::authorize(
                    self.credentials.as_ref(),
                    BedrockCredentialContext {
                        scope: &context.scope,
                        audience: BedrockAudience::Metadata,
                        region: &options.region,
                        cancellation: &context.cancellation,
                        deadline: context.deadline,
                    },
                    auth::SigningRequest {
                        method: "GET",
                        url: &url,
                        body: &[],
                        service: "bedrock",
                        clock: self.connection.0.clock.as_ref(),
                        headers,
                    },
                )
                .await?;
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(url)
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|_| unavailable())?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::Unverified,
                        evidence_ref: Id::new("aws.bedrock.metadata")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(unavailable());
                }
                let mut bytes = vec![];
                while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(options.max_transport_bytes)
                    {
                        return Err(unavailable());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|s| parse_json(s).ok())
                    .ok_or_else(unavailable)?;
                let (model, availability, revision) = if profile {
                    if text(&body, "inferenceProfileId")? != options.selector.value()
                        && text(&body, "inferenceProfileArn")? != options.selector.value()
                    {
                        return Err(drift());
                    }
                    let models = body
                        .get("models")
                        .and_then(Value::as_array)
                        .filter(|m| !m.is_empty())
                        .ok_or_else(unavailable)?;
                    let mut actual = None;
                    for model in models {
                        let (region, id) = foundation_arn(text(model, "modelArn")?)?;
                        if !options.allowed_destination_regions.is_empty()
                            && !options
                                .allowed_destination_regions
                                .iter()
                                .any(|v| v == region)
                        {
                            return Err(error(ErrorCode::AccessDenied, "profile_destination"));
                        }
                        if actual.is_some_and(|old| old != id) {
                            return Err(drift());
                        }
                        actual = Some(id);
                    }
                    let revision = canonical_digest(
                        &json!({"arn":body.get("inferenceProfileArn"),"models":models,"status":body.get("status"),"updated_at":body.get("updatedAt")}),
                    );
                    (
                        Id::new(actual.ok_or_else(unavailable)?)?,
                        if text(&body, "status")? == "ACTIVE" {
                            ModelRouteAvailability::Available
                        } else {
                            ModelRouteAvailability::Unknown
                        },
                        Some(Id::new(revision.as_str())?),
                    )
                } else {
                    let details = body.get("modelDetails").ok_or_else(unavailable)?;
                    let id = text(details, "modelId")?;
                    let arn = text(details, "modelArn")?;
                    if options.selector.value() != id && options.selector.value() != arn {
                        return Err(drift());
                    }
                    let (_, arn_id) = foundation_arn(arn)?;
                    if arn_id != id || text(details, "providerName")? != "Anthropic" {
                        return Err(drift());
                    }
                    if details.get("responseStreamingSupported") != Some(&json!(true)) {
                        return Err(error(ErrorCode::ModelCapabilityUnsupported, "streaming"));
                    }
                    let available = match details
                        .pointer("/modelLifecycle/status")
                        .and_then(Value::as_str)
                    {
                        Some("ACTIVE" | "LEGACY") => ModelRouteAvailability::Available,
                        _ => ModelRouteAvailability::Unknown,
                    };
                    (Id::new(id)?, available, None)
                };
                let snapshot = self.snapshots.get(&model);
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability,
                    model_id: Some(model.clone()),
                    model_version: snapshot.map(|s| s.model_version.clone()),
                    deployment_revision: revision,
                    version_semantics: if profile {
                        VersionSemantics::MutableDeployment
                    } else if snapshot.is_some() {
                        VersionSemantics::Pinned
                    } else {
                        VersionSemantics::Unverified
                    },
                    evidence_ref: snapshot
                        .map(|s| s.evidence_ref.clone())
                        .unwrap_or(Id::new("aws.bedrock.metadata")?),
                })
            };
            tokio::select! {biased;
                _=context.cancellation.cancelled()=>Err(error(ErrorCode::Cancelled,"inspection")),
                _=tokio::time::sleep_until(context.deadline)=>Err(error(ErrorCode::DeadlineExceeded,"inspection")),
                result=operation=>result,
            }
        })
    }
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str, ContractError> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(unavailable)
}
fn foundation_arn(value: &str) -> Result<(&str, &str), ContractError> {
    let fields: Vec<_> = value.splitn(6, ':').collect();
    if fields.len() != 6
        || fields[0] != "arn"
        || !matches!(
            fields[1],
            "aws" | "aws-cn" | "aws-us-gov" | "aws-iso" | "aws-iso-b" | "aws-iso-e" | "aws-iso-f"
        )
        || fields[2] != "bedrock"
        || fields[3].is_empty()
    {
        return Err(unavailable());
    }
    let id = fields[5]
        .strip_prefix("foundation-model/")
        .filter(|v| !v.is_empty() && !v.contains('/'))
        .ok_or_else(unavailable)?;
    Ok((fields[3], id))
}
fn unavailable() -> ContractError {
    error(ErrorCode::ModelInspectionUnavailable, "metadata")
}
fn drift() -> ContractError {
    error(ErrorCode::ModelVersionDrift, "metadata_target")
}
