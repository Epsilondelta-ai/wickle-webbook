use std::fmt;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    ContractError, Id, JsonObject,
    serialization::{decode, optional},
};

/// Boxed, Send future used by dynamically injected ports.
pub type PortFuture<'a, T> = futures_util::future::BoxFuture<'a, Result<T, ContractError>>;

/// Boxed, Send stream used by dynamically injected ports.
pub type PortStream<'a, T> = futures_util::stream::BoxStream<'a, Result<T, ContractError>>;

/// Opaque resource scope. Deserializing it does not authenticate a caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Host tenant identifier.
    pub tenant_id: Id,
    /// Host workspace identifier.
    pub workspace_id: Id,
    /// Optional user scope; explicit null is rejected.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub user_id: Option<Id>,
}

/// Owned tool inputs supplied by the Host, redacted from Debug output.
/// Serialization is for protected storage, never automatic model projection.
#[derive(Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SystemInputs(JsonObject);

impl SystemInputs {
    /// Take ownership of the supplied input map.
    pub fn new(values: JsonObject) -> Self {
        Self(values)
    }
    /// Explicit access for an authorized binder or storage implementation.
    pub fn values(&self) -> &JsonObject {
        &self.0
    }
}

impl fmt::Debug for SystemInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SystemInputs(<redacted>)")
    }
}

/// Serializable Host context data, separate from cancellation and runtime clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContextData {
    /// Authenticated resource scope supplied by the Host.
    pub scope: Scope,
    /// Host-authenticated principal reference.
    pub principal_ref: Id,
    /// Current capability grant reference, not a grant issued by the model.
    pub capability_grant_ref: Id,
    /// Optional tracing correlation data.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub trace_context: Option<JsonObject>,
    /// Missing means start defaults to empty, resume uses the stored snapshot.
    /// An explicit empty object is preserved; null is rejected.
    #[serde(
        default,
        deserialize_with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_inputs: Option<SystemInputs>,
}

impl ExecutionContextData {
    /// Decode context data without authenticating it or creating runtime objects.
    pub fn from_json(input: &str) -> Result<Self, ContractError> {
        decode(input, None)
    }
}

/// Host-owned runtime context; it cannot be constructed by deserializing JSON.
///
/// ```compile_fail
/// fn persist_runtime(context: &wickle::ExecutionContext) {
///     serde_json::to_string(context).unwrap();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Owned, authenticated context data.
    pub data: ExecutionContextData,
    /// A runtime signal, never part of profile or checkpoint JSON.
    pub cancellation: CancellationToken,
}

impl ExecutionContext {
    /// Attach a Host-provided cancellation signal to owned context data.
    pub fn new(data: ExecutionContextData, cancellation: CancellationToken) -> Self {
        Self { data, cancellation }
    }
}
