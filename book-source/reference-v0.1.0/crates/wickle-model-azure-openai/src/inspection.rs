use crate::{
    AzureAudience, AzureCredentialContext, AzureCredentialProvider, AzureOpenAiConnection, auth,
    connection::origin, error,
};
use serde_json::{Value, json};
use std::sync::Arc;
use wickle::*;

/// Azure Resource Manager connection for deployment metadata, separate from inference.
#[derive(Debug, Clone)]
pub struct AzureInspectionOptions {
    /// Management origin; supports explicit loopback HTTP for transport tests.
    pub endpoint: String,
    /// Exact ARM protocol version, unrelated to the model or inference API version.
    pub api_version: String,
}
impl Default for AzureInspectionOptions {
    fn default() -> Self {
        Self {
            endpoint: "https://management.azure.com/".into(),
            api_version: "2025-06-01".into(),
        }
    }
}

/// Observes the actual model release behind a mutable Azure deployment.
#[derive(Clone)]
pub struct AzureOpenAiInspector {
    connection: AzureOpenAiConnection,
    credentials: Arc<dyn AzureCredentialProvider>,
    url: reqwest::Url,
    resource_id: String,
}
impl AzureOpenAiInspector {
    /// Configure an ARM GET with Host-supplied management credentials.
    /// No request occurs until `inspect`; a data-plane API key cannot authorize ARM.
    pub fn new(
        connection: AzureOpenAiConnection,
        credentials: Arc<dyn AzureCredentialProvider>,
        options: AzureInspectionOptions,
    ) -> Result<Self, ContractError> {
        // This schema is verified against the selected protocol generation only.
        if options.api_version != "2025-06-01" {
            return Err(error(
                ErrorCode::InvalidConfiguration,
                "management_api_version",
            ));
        }
        let account = connection
            .0
            .options
            .resource_id
            .as_ref()
            .ok_or_else(|| error(ErrorCode::InvalidConfiguration, "resource_id"))?;
        let resource_id = format!("{account}/deployments/{}", connection.0.options.deployment);
        let mut url = origin(&options.endpoint)?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| error(ErrorCode::InvalidConfiguration, "management_endpoint"))?;
            segments.clear();
            for segment in resource_id.trim_start_matches('/').split('/') {
                segments.push(segment);
            }
        }
        url.query_pairs_mut()
            .append_pair("api-version", &options.api_version);
        Ok(Self {
            connection,
            credentials,
            url,
            resource_id,
        })
    }
}
impl ModelRouteInspector for AzureOpenAiInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        context: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            self.connection.validate(route, &context.scope)?;
            let operation = async {
                let headers = auth::authorize(
                    self.credentials.as_ref(),
                    AzureCredentialContext {
                        scope: &context.scope,
                        audience: AzureAudience::Management,
                        cancellation: &context.cancellation,
                        deadline: context.deadline,
                    },
                )
                .await?;
                let mut response = self
                    .connection
                    .0
                    .client
                    .get(self.url.clone())
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|_| {
                        error(ErrorCode::ModelInspectionUnavailable, "management_request")
                    })?;
                if response.status().as_u16() == 404 {
                    return Ok(ModelRouteObservation {
                        route_digest: route.digest(),
                        availability: ModelRouteAvailability::Unavailable,
                        model_id: None,
                        model_version: None,
                        deployment_revision: None,
                        version_semantics: VersionSemantics::MutableDeployment,
                        evidence_ref: Id::new("azure.arm.deployments.get")?,
                    });
                }
                if !response.status().is_success() {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "management_status",
                    ));
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| error(ErrorCode::ModelInspectionUnavailable, "management_body"))?
                {
                    if bytes.len().saturating_add(chunk.len())
                        > 65_536.min(self.connection.0.options.max_transport_bytes)
                    {
                        return Err(error(
                            ErrorCode::ModelInspectionUnavailable,
                            "management_limit",
                        ));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let body = std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(|text| parse_json(text).ok())
                    .ok_or_else(|| {
                        error(ErrorCode::ModelInspectionUnavailable, "management_json")
                    })?;
                if !body
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(&self.resource_id))
                    || body.get("name").and_then(Value::as_str)
                        != Some(self.connection.0.options.deployment.as_str())
                    || !body
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|value| {
                            value.eq_ignore_ascii_case(
                                "Microsoft.CognitiveServices/accounts/deployments",
                            )
                        })
                {
                    return Err(error(ErrorCode::ModelVersionDrift, "management_target"));
                }
                if body
                    .pointer("/properties/model/format")
                    .and_then(Value::as_str)
                    != Some("OpenAI")
                {
                    return Err(error(
                        ErrorCode::ModelInspectionUnavailable,
                        "management_model_format",
                    ));
                }
                let model_id = identifier(&body, "/properties/model/name")?;
                let model_version = identifier(&body, "/properties/model/version")?;
                let availability = match body
                    .pointer("/properties/provisioningState")
                    .and_then(Value::as_str)
                {
                    Some("Succeeded") => ModelRouteAvailability::Available,
                    Some("Failed" | "Canceled" | "Deleted") => ModelRouteAvailability::Unavailable,
                    _ => ModelRouteAvailability::Unknown,
                };
                // A metadata fingerprint detects observed changes; it does not
                // make a mutable deployment immutable or prevent later upgrades.
                let revision = canonical_digest(&json!({
                    "resource_id": self.resource_id,
                    "model": body.pointer("/properties/model"),
                    "upgrade": body.pointer("/properties/versionUpgradeOption"),
                    "etag": body.get("etag"),
                }));
                Ok(ModelRouteObservation {
                    route_digest: route.digest(),
                    availability,
                    model_id: Some(model_id),
                    model_version: Some(model_version),
                    deployment_revision: Some(Id::new(revision.as_str())?),
                    version_semantics: VersionSemantics::MutableDeployment,
                    evidence_ref: Id::new("azure.arm.deployments.get.2025-06-01")?,
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
fn identifier(value: &Value, path: &str) -> Result<Id, ContractError> {
    value
        .pointer(path)
        .and_then(Value::as_str)
        .and_then(|value| Id::new(value).ok())
        .ok_or_else(|| error(ErrorCode::ModelInspectionUnavailable, "management_model"))
}
