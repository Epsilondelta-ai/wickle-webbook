use crate::{VertexAudience, VertexConnection, error};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use wickle::*;
/// Host evidence linking a publisher artifact to a documented inference release.
#[derive(Debug, Clone)]
pub struct VertexSnapshot {
    /// Exact model selector registered in the catalog.
    pub model_id: Id,
    /// Documented inference modelVersion identity.
    pub model_version: Id,
    /// Expected PublisherModel.versionId, not inferred to equal model_version.
    pub publisher_version_id: Id,
    /// Locations supported by the cited release metadata.
    pub supported_locations: Vec<String>,
    /// Host-owned reference to the supporting evidence.
    pub evidence_ref: Id,
}
/// Publisher metadata inspection; availability is not proof of inference permission.
#[derive(Clone)]
pub struct VertexInspector {
    connection: VertexConnection,
    snapshots: Arc<BTreeMap<Id, VertexSnapshot>>,
}
impl VertexInspector {
    /// Register known releases without making a network request.
    pub fn new(
        connection: VertexConnection,
        snapshots: Vec<VertexSnapshot>,
    ) -> Result<Self, ContractError> {
        let mut known = BTreeMap::new();
        for snapshot in snapshots {
            if snapshot.supported_locations.is_empty()
                || snapshot.supported_locations.iter().any(|s| {
                    s.is_empty()
                        || !s
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                })
                || known.insert(snapshot.model_id.clone(), snapshot).is_some()
            {
                return Err(error(ErrorCode::InvalidConfiguration, "snapshots"));
            }
        }
        Ok(Self {
            connection,
            snapshots: Arc::new(known),
        })
    }
}
impl ModelRouteInspector for VertexInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let name = crate::connection::model_name(route.model_id.as_str())?;
            let mut url = self
                .connection
                .0
                .metadata_base
                .join(&format!("v1/publishers/google/models/{name}"))
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "metadata_url"))?;
            url.query_pairs_mut()
                .append_pair("view", "PUBLISHER_MODEL_VERSION_VIEW_BASIC");
            let operation =
                async {
                    let headers = self
                        .connection
                        .headers(
                            VertexAudience::Metadata,
                            &context.cancellation,
                            context.deadline,
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
                        .map_err(|_| {
                            error(ErrorCode::ModelInspectionUnavailable, "metadata_request")
                        })?;
                    if response.status().as_u16() == 404 {
                        return Ok(ModelRouteObservation {
                            route_digest: route.digest(),
                            availability: ModelRouteAvailability::Unavailable,
                            model_id: None,
                            model_version: None,
                            deployment_revision: None,
                            version_semantics: VersionSemantics::Unverified,
                            evidence_ref: Id::new("vertex.publishers.models.get")?,
                        });
                    }
                    if !response.status().is_success() {
                        return Err(error(
                            ErrorCode::ModelInspectionUnavailable,
                            "metadata_status",
                        ));
                    }
                    let mut bytes = vec![];
                    while let Some(chunk) = response.chunk().await.map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "metadata_body")
                    })? {
                        if bytes.len().saturating_add(chunk.len())
                            > 65_536.min(self.connection.0.options.max_transport_bytes)
                        {
                            return Err(error(
                                ErrorCode::ModelInspectionUnavailable,
                                "metadata_limit",
                            ));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    let body = parse_json(std::str::from_utf8(&bytes).map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "metadata_json")
                    })?)?;
                    if body.get("name").and_then(Value::as_str)
                        != Some(format!("publishers/google/models/{name}").as_str())
                    {
                        return Err(error(ErrorCode::ModelVersionDrift, "metadata_name"));
                    }
                    let snapshot = self.snapshots.get(&route.model_id);
                    if snapshot.is_some_and(|s| {
                        body.get("versionId").and_then(Value::as_str)
                            != Some(s.publisher_version_id.as_str())
                    }) {
                        return Err(error(ErrorCode::ModelVersionDrift, "publisher_version"));
                    }
                    let availability = match snapshot {
                        Some(s)
                            if s.supported_locations
                                .contains(&self.connection.0.options.location) =>
                        {
                            ModelRouteAvailability::Available
                        }
                        Some(_) => ModelRouteAvailability::Unavailable,
                        None => ModelRouteAvailability::Unknown,
                    };
                    Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability,
                        model_id: Some(route.model_id.clone()),
                        model_version: snapshot.map(|s| s.model_version.clone()),
                        deployment_revision: None,
                        version_semantics: if snapshot.is_some() {
                            VersionSemantics::Pinned
                        } else {
                            VersionSemantics::Unverified
                        },
                        evidence_ref: snapshot.map_or_else(
                            || Id::new("vertex.publishers.models.get"),
                            |s| Ok(s.evidence_ref.clone()),
                        )?,
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
