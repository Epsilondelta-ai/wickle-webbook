//! OpenAI Responses over bounded HTTP/SSE, with Host-supplied credentials.
//!
//! A model stream represents exactly one POST. SDK retries, redirects, native
//! provider tools, conversation storage, and automatic truncation are disabled.

#![forbid(unsafe_code)]

mod connection;
mod inspection;
mod model;

pub use connection::{OpenAiConnection, OpenAiOptions};
pub use inspection::{OpenAiInspector, OpenAiSnapshot};
pub use model::OpenAiModel;

use wickle::{ContractError, ErrorCode};

fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("openai.{location}"))
}
