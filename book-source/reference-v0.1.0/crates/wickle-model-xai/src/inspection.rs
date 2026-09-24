use crate::{XaiConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;

/// A provider-documented immutable model release, registered by the Host.
/// API availability alone, a date in a name, or a requested version is not proof.
#[derive(Debug, Clone)]
pub struct XaiSnapshot {
    /// Exact provider model identifier in the cited snapshot metadata.
    pub model_id: Id,
    /// Release identity established by that metadata.
    pub model_version: Id,
    /// Host-owned reference to the documentation or metadata establishing immutability.
    pub evidence_ref: Id,
}

/// Current account availability combined with explicit immutable-release evidence.
/// Unregistered model identifiers retain unknown release and unverified semantics.
#[derive(Clone)]
pub struct XaiInspector {
    connection: XaiConnection,
    snapshots: Arc<BTreeMap<Id, XaiSnapshot>>,
}
impl XaiInspector {
    /// Build an inspector without a network call. Duplicate identifiers are rejected.
    pub fn new(
        connection: XaiConnection,
        snapshots: Vec<XaiSnapshot>,
    ) -> Result<Self, ContractError> {
        let mut known = BTreeMap::new();
        for snapshot in snapshots {
            if known.insert(snapshot.model_id.clone(), snapshot).is_some() {
                return Err(error(ErrorCode::InvalidConfiguration, "snapshots"));
            }
        }
        Ok(Self {
            connection,
            snapshots: Arc::new(known),
        })
    }
}
impl ModelRouteInspector for XaiInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let mut url = self
                .connection
                .0
                .base
                .join("models/")
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?;
            url.path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "models_url"))?
                .pop_if_empty()
                .push(route.model_id.as_str());
            let operation = async {
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(url)
                    .send()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_request"))?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::Unverified,
                        evidence_ref: Id::new("xai.models.retrieve")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "models_status",
                    ));
                }
                let mut bytes = vec![];
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_body"))?
                {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        return Err(error(ErrorCode::ModelInspectionUnavailable, "models_limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body =
                    parse_json(std::str::from_utf8(&bytes).map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "models_json")
                    })?)
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "models_json"))?;
                if body.get("object").and_then(Value::as_str) != Some("model") {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "models_object",
                    ));
                }
                let model_id = body
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error(ErrorCode::ModelInspectionUnavailable, "models_id"))?;
                if model_id != route.model_id.as_str() {
                    return Err(error(ErrorCode::ModelVersionDrift, "models_id"));
                }
                let snapshot = self.snapshots.get(&route.model_id);
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability: ModelRouteAvailability::Available,
                    model_id: Some(Id::new(model_id)?),
                    model_version: snapshot.map(|value| value.model_version.clone()),
                    deployment_revision: None,
                    version_semantics: if snapshot.is_some() {
                        VersionSemantics::Pinned
                    } else {
                        VersionSemantics::Unverified
                    },
                    evidence_ref: snapshot.map_or_else(
                        || Id::new("xai.models.retrieve"),
                        |value| Ok(value.evidence_ref.clone()),
                    )?,
                })
            };
            tokio::select! { biased;
                _ = context.cancellation.cancelled() => Err(error(ErrorCode::Cancelled, "inspection")),
                _ = tokio::time::sleep_until(context.deadline) => Err(error(ErrorCode::DeadlineExceeded, "inspection")),
                result = operation => result,
            }
        })
    }
}
