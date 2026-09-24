//! xAI Grok Responses with explicit scoped credentials and bounded replay.
#![forbid(unsafe_code)]
mod connection;
mod inspection;
mod model;
pub use connection::{XaiConnection, XaiOptions};
pub use inspection::{XaiInspector, XaiSnapshot};
pub use model::XaiModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("xai.{location}"))
}
