//! AWS Bedrock Claude with explicit credentials, endpoint contracts and bounded streams.
#![forbid(unsafe_code)]
mod auth;
mod connection;
mod framing;
mod inspection;
mod model;
pub use auth::{
    BedrockAudience, BedrockCredential, BedrockCredentialContext, BedrockCredentialProvider,
};
pub use aws_credential_types::Credentials as AwsCredentials;
pub use connection::{
    BedrockConnection, BedrockEndpoint, BedrockOperation, BedrockOptions, BedrockSelector,
};
pub use inspection::{BedrockInspector, BedrockSnapshot};
pub use model::BedrockModel;
use wickle::{ContractError, ErrorCode};
fn error(code: ErrorCode, location: &str) -> ContractError {
    ContractError::new(code, format!("bedrock.{location}"))
}
