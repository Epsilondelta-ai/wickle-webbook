//! Gemini generateContent with explicit API versions, credentials and bounded streams.
#![forbid(unsafe_code)]
mod codec;
mod connection;
mod inspection;
mod model;
mod raw;
mod response;
mod schema;
pub use connection::{GeminiConnection, GeminiOptions};
pub use inspection::{GeminiInspector, GeminiSnapshot};
pub use model::GeminiModel;
/// generateContent wire primitives for independently authenticated platform adapters.
pub mod protocol {
    pub use crate::codec::{FunctionSchemaFormat, encode_request, encode_vertex_request};
    pub use crate::response::Decoder as GenerateContentDecoder;
    pub use crate::schema::GeminiToolSchemaCompiler;
}
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("gemini.{location}"))
}
