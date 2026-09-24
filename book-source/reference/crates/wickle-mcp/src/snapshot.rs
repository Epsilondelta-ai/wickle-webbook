use crate::{client::PROTOCOL_VERSION, error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt, num::NonZeroU64};
use wickle::*;
/// Raw discovery result for protected Host review. It grants no execution permission.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSnapshot {
    scope: Scope,
    connection_ref: VersionedRef,
    protocol: String,
    server: Value,
    tools: BTreeMap<String, Value>,
}
impl fmt::Debug for McpSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpSnapshot")
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}
/// Explicit Host choices. Input ownership and effects are never inferred from names or hints.
#[derive(Clone, Debug)]
pub struct McpToolApproval {
    /// Host-assigned stable Tool identity.
    pub tool_id: Id,
    /// Portable model-facing name; separate from the original MCP name.
    pub model_name: Id,
    /// Explicit model-owned property names, including an intentional empty list.
    pub agent_parameters: Vec<String>,
    /// Optional hidden parameter -> registered system key mapping.
    pub system_bindings: Option<BTreeMap<String, Id>>,
    /// Optional reviewed description replacing the remote description.
    pub description: Option<String>,
    /// Trusted Host classification; defaults to Unknown.
    pub side_effect: ToolSideEffect,
    /// Bounded complete output size.
    pub max_output_bytes: NonZeroU64,
}
impl McpToolApproval {
    /// Start with no retry or effect assumptions. The caller must select input ownership.
    pub fn new(tool_id: Id, model_name: Id, agent_parameters: Vec<String>) -> Self {
        Self {
            tool_id,
            model_name,
            agent_parameters,
            system_bindings: None,
            description: None,
            side_effect: ToolSideEffect::Unknown,
            max_output_bytes: NonZeroU64::new(1024 * 1024).expect("nonzero"),
        }
    }
}
impl McpSnapshot {
    pub(crate) fn new(
        scope: Scope,
        connection_ref: VersionedRef,
        server: Value,
        tools: Vec<Value>,
    ) -> Result<Self, ContractError> {
        if !server
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
            || !server
                .get("version")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        {
            return Err(error(ErrorCode::InvalidContract, "server_identity"));
        }
        let mut map = BTreeMap::new();
        for raw in tools {
            let parsed: rmcp::model::Tool = serde_json::from_value(raw.clone())
                .map_err(|_| error(ErrorCode::InvalidToolInputContract, "descriptor"))?;
            let name = parsed.name.to_string();
            if name.is_empty()
                || name.len() > 256
                || name.chars().any(char::is_control)
                || map.insert(name, raw).is_some()
            {
                return Err(error(ErrorCode::InvalidToolInputContract, "tool_name"));
            }
        }
        Ok(Self {
            scope,
            connection_ref,
            protocol: PROTOCOL_VERSION.into(),
            server,
            tools: map,
        })
    }
    /// Scope for which discovery was authorized.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Exact Host connection revision observed during discovery.
    pub fn connection_ref(&self) -> &VersionedRef {
        &self.connection_ref
    }
    /// Original server implementation metadata; never automatically sent to the model.
    pub fn server_info(&self) -> &Value {
        &self.server
    }
    /// Original remote names, without automatically creating model tools.
    pub fn tool_names(&self) -> impl Iterator<Item = &str> {
        self.tools.keys().map(String::as_str)
    }
    /// Privileged access to the complete raw descriptor, including unknown metadata.
    pub fn raw_tool(&self, name: &str) -> Option<&Value> {
        self.tools.get(name)
    }
    /// Integrity identity for protected persistence and review.
    pub fn digest(&self) -> JsonDigest {
        canonical_digest(&serde_json::to_value(self).expect("data snapshot"))
    }
    /// Restore only against a trusted expected digest and owner/connection identity.
    pub fn restore(
        input: &str,
        scope: &Scope,
        connection: &VersionedRef,
        expected: &JsonDigest,
    ) -> Result<Self, ContractError> {
        let value: Self = serde_json::from_value(parse_json(input)?)
            .map_err(|_| error(ErrorCode::InvalidContract, "snapshot"))?;
        if value.digest() != *expected
            || value.scope != *scope
            || value.connection_ref != *connection
            || value.protocol != PROTOCOL_VERSION
        {
            return Err(error(ErrorCode::InvalidReference, "snapshot"));
        }
        let rebuilt = Self::new(
            value.scope.clone(),
            value.connection_ref.clone(),
            value.server.clone(),
            value.tools.values().cloned().collect(),
        )?;
        if rebuilt.tools != value.tools {
            return Err(error(ErrorCode::InvalidContract, "snapshot_names"));
        }
        Ok(value)
    }
    /// Exact version of one original Tool contract and server identity.
    pub fn tool_version(&self, name: &str) -> Result<Id, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Id::new(
            canonical_digest(&json!({"protocol":self.protocol,"server":self.server,"tool":raw}))
                .as_str(),
        )
    }
    /// Convert one reviewed Tool into a core descriptor. Compile it with the core's
    /// ToolSchemaCompiler and registered system definitions before activation.
    pub fn descriptor(
        &self,
        name: &str,
        approval: McpToolApproval,
    ) -> Result<ToolDescriptor, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Ok(ToolDescriptor {
            tool: VersionedRef {
                id: approval.tool_id,
                version: self.tool_version(name)?,
            },
            name: approval.model_name,
            description: approval.description.unwrap_or_else(|| {
                raw.get("description")
                    .and_then(Value::as_str)
                    .unwrap_or(name)
                    .into()
            }),
            input_schema: raw["inputSchema"].clone(),
            agent_parameters: approval.agent_parameters,
            system_bindings: approval.system_bindings,
            output_schema: self.output_schema(name)?,
            side_effect: approval.side_effect,
            concurrency: ToolConcurrency::Serial,
            retry: ToolRetryPolicy::Never,
            reconcile: false,
            max_output_bytes: approval.max_output_bytes,
        })
    }
    pub(crate) fn output_schema(&self, name: &str) -> Result<Value, ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        Ok(raw.get("outputSchema").filter(|v|!v.is_null()).cloned().unwrap_or_else(||json!({"type":"object","properties":{"content":{"type":"array","items":{"type":"object","properties":{"type":{"const":"text"},"text":{"type":"string"}},"required":["type","text"],"additionalProperties":false}}},"required":["content"],"additionalProperties":false})))
    }
    pub(crate) fn validate_descriptor(
        &self,
        name: &str,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ContractError> {
        let raw = self
            .tools
            .get(name)
            .ok_or_else(|| error(ErrorCode::InvalidReference, "tool"))?;
        if descriptor.tool.version != self.tool_version(name)?
            || descriptor.input_schema != raw["inputSchema"]
            || descriptor.output_schema != self.output_schema(name)?
            || descriptor.concurrency != ToolConcurrency::Serial
            || descriptor.retry != ToolRetryPolicy::Never
            || descriptor.reconcile
        {
            return Err(error(
                ErrorCode::InvalidToolInputContract,
                "approved_descriptor",
            ));
        }
        Ok(())
    }
    pub(crate) fn matches_tool(&self, current: &Self, name: &str) -> bool {
        self.scope == current.scope
            && self.connection_ref == current.connection_ref
            && self.protocol == current.protocol
            && self.server == current.server
            && self.tools.contains_key(name)
            && self.tools.get(name) == current.tools.get(name)
    }
}
