//! Vertex AI Gemini with Host-owned OAuth credentials and explicit resource targets.
#![forbid(unsafe_code)]
mod auth;
mod connection;
mod inspection;
mod model;
pub use auth::{VertexAudience, VertexToken, VertexTokenContext, VertexTokenProvider};
pub use connection::{VertexConnection, VertexOptions};
pub use inspection::{VertexInspector, VertexSnapshot};
pub use model::VertexModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("vertex.{location}"))
}
