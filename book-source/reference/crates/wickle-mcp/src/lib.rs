//! Scope-bound, reviewed MCP Tools over bounded stdio connections.
#![forbid(unsafe_code)]
mod client;
mod executor;
mod factory;
mod snapshot;
mod transport;
pub use client::{McpClient, McpCommand, McpLimits};
pub use executor::McpToolExecutor;
pub use factory::{McpAdapterFactory, McpExport};
pub use snapshot::{McpSnapshot, McpToolApproval};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("mcp.{location}"))
}
