//! Azure OpenAI Responses v1 with Host-owned authorization and explicit deployment identity.
//! Model metadata uses a separate Azure Resource Manager authorization path.
#![forbid(unsafe_code)]
mod auth;
mod connection;
mod inspection;
mod model;
pub use auth::{AzureAudience, AzureCredential, AzureCredentialContext, AzureCredentialProvider};
pub use connection::{AzureOpenAiConnection, AzureOpenAiOptions};
pub use inspection::{AzureInspectionOptions, AzureOpenAiInspector};
pub use model::AzureOpenAiModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("azure_openai.{location}"))
}
