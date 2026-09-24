//! Shared Responses wire codecs. Provider authentication, endpoints, model
//! selection, and capability policy belong to the calling adapter and Host.
#![forbid(unsafe_code)]
mod codec;
mod response;
mod schema;
mod sse;
pub use codec::{encode_request, encode_xai_request};
pub use response::Decoder as ResponsesDecoder;
pub use schema::{AzureResponsesToolSchemaCompiler, ResponsesToolSchemaCompiler};
pub use sse::{Decoder as SseDecoder, Event as SseEvent};
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("responses.{location}"))
}
