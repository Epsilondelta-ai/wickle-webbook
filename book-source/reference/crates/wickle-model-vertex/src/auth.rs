use crate::error;
use std::{fmt, time::SystemTime};
use wickle::*;
/// Purpose for which the Host resolves an OAuth access token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VertexAudience {
    /// Generate content.
    Inference,
    /// Inspect publisher metadata.
    Metadata,
}
/// Explicit authorization context. Loading ADC remains the Host's responsibility.
pub struct VertexTokenContext<'a> {
    /// Connection owner.
    pub scope: &'a Scope,
    /// Resource project.
    pub project: &'a str,
    /// Request location, including global or a multi-region.
    pub location: &'a str,
    /// Inference or metadata operation.
    pub audience: VertexAudience,
    /// Cancellation while refreshing credentials.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Deadline covering credential resolution and HTTP.
    pub deadline: tokio::time::Instant,
}
/// Access token and optional actual expiry, with redacted Debug output.
#[derive(Clone)]
pub struct VertexToken {
    pub(crate) value: String,
    pub(crate) expires_at: Option<SystemTime>,
}
impl VertexToken {
    /// Construct from credentials obtained by the Host's chosen ADC/OAuth library.
    pub fn new(
        value: impl Into<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<Self, ContractError> {
        let value = value.into();
        if value.is_empty() || value.chars().any(char::is_whitespace) {
            return Err(error(ErrorCode::AccessDenied, "token"));
        }
        Ok(Self { value, expires_at })
    }
}
impl fmt::Debug for VertexToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VertexToken([redacted])")
    }
}
/// Resolve or refresh a token without putting a Google SDK in the core.
pub trait VertexTokenProvider: Send + Sync {
    /// Called once for a physical request, within cancellation/deadline bounds.
    fn token<'a>(&'a self, context: &'a VertexTokenContext<'a>) -> PortFuture<'a, VertexToken>;
}
impl VertexTokenProvider for VertexToken {
    fn token<'a>(&'a self, _: &'a VertexTokenContext<'a>) -> PortFuture<'a, VertexToken> {
        Box::pin(async { Ok(self.clone()) })
    }
}
