use crate::error;
use reqwest::header::{HeaderMap, HeaderValue};
use std::fmt;
use wickle::*;

/// The authentication audience requested by this adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzureAudience {
    /// Azure OpenAI inference endpoint.
    Inference,
    /// Azure Resource Manager deployment metadata.
    Management,
}

/// Host-owned authorization lookup, with a bounded lifetime and explicit scope.
pub struct AzureCredentialContext<'a> {
    /// Connection owner; credentials must be authorized for this scope.
    pub scope: &'a Scope,
    /// Service audience; management always requires an Entra token.
    pub audience: AzureAudience,
    /// Cooperative cancellation while obtaining or refreshing a token.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Effective deadline for authorization and its HTTP request.
    pub deadline: tokio::time::Instant,
}

/// Credential material is supplied by the Host and never written to a catalog.
#[derive(Clone)]
pub enum AzureCredential {
    /// Resource API key, sent in the `api-key` header for inference only.
    ApiKey(String),
    /// Entra access token for the requested audience, refreshed by the Host.
    EntraToken(String),
}
impl fmt::Debug for AzureCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ApiKey(_) => "AzureCredential::ApiKey([redacted])",
            Self::EntraToken(_) => "AzureCredential::EntraToken([redacted])",
        })
    }
}

/// Called once per physical request; the Host owns token acquisition and renewal.
pub trait AzureCredentialProvider: Send + Sync {
    /// Return credentials for the connection scope and requested service audience.
    fn credential<'a>(
        &'a self,
        context: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential>;
}
impl AzureCredentialProvider for AzureCredential {
    fn credential<'a>(
        &'a self,
        _: &'a AzureCredentialContext<'a>,
    ) -> PortFuture<'a, AzureCredential> {
        Box::pin(async { Ok(self.clone()) })
    }
}

pub(crate) async fn authorize(
    provider: &dyn AzureCredentialProvider,
    context: AzureCredentialContext<'_>,
) -> Result<HeaderMap, ContractError> {
    let credential = tokio::select! { biased;
        _ = context.cancellation.cancelled() => return Err(error(ErrorCode::Cancelled, "credential")),
        _ = tokio::time::sleep_until(context.deadline) => return Err(error(ErrorCode::DeadlineExceeded, "credential")),
        result = provider.credential(&context) => result.map_err(|_| error(ErrorCode::AccessDenied, "credential"))?,
    };
    let (name, value) = match credential {
        AzureCredential::ApiKey(key) if context.audience == AzureAudience::Inference => {
            ("api-key", key)
        }
        AzureCredential::EntraToken(token) => {
            validate(&token)?;
            ("authorization", format!("Bearer {token}"))
        }
        _ => return Err(error(ErrorCode::AccessDenied, "credential_audience")),
    };
    // HeaderValue's sensitive bit also keeps HTTP diagnostic formatting redacted.
    if name == "api-key" {
        validate(&value)?;
    }
    let mut value = HeaderValue::from_str(&value)
        .map_err(|_| error(ErrorCode::AccessDenied, "credential_format"))?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(name, value);
    Ok(headers)
}
fn validate(value: &str) -> Result<(), ContractError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(error(ErrorCode::AccessDenied, "credential_format"));
    }
    Ok(())
}
