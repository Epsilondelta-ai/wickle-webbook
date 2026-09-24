use crate::{McpClient, McpCommand, McpLimits, McpSnapshot, error};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use wickle::*;
/// One Host-approved export, retaining the original remote name and compiled contract.
#[derive(Clone)]
pub struct McpExport {
    /// Adapter-local export identifier.
    pub export_id: Id,
    /// Original MCP Tool name, never a model-supplied execution target.
    pub remote_name: String,
    /// Original compiled descriptor before an optional profile alias.
    pub compiled: CompiledTool,
}
/// Opens one scoped stdio client per execution segment from Host-owned configuration.
pub struct McpAdapterFactory {
    definition: AdapterDefinition,
    connection_name: Id,
    command: McpCommand,
    limits: McpLimits,
    snapshot: McpSnapshot,
    exports: BTreeMap<Id, McpExport>,
}
impl McpAdapterFactory {
    /// Register reviewed metadata and compiled exports. Remote discovery cannot add
    /// capabilities or executable paths to this immutable factory.
    pub fn new(
        definition: AdapterDefinition,
        connection_name: Id,
        command: McpCommand,
        limits: McpLimits,
        snapshot: McpSnapshot,
        exports: Vec<McpExport>,
    ) -> Result<Self, ContractError> {
        if definition.metadata.reference.kind != ComponentKind::Adapter
            || definition.metadata.reference.version.is_none()
            || definition.metadata.required_connections != BTreeSet::from([connection_name.clone()])
        {
            return Err(error(ErrorCode::InvalidReference, "connection_definition"));
        }
        let mut selected = BTreeMap::new();
        for export in exports {
            snapshot.validate_descriptor(&export.remote_name, export.compiled.descriptor())?;
            if selected.insert(export.export_id.clone(), export).is_some() {
                return Err(error(ErrorCode::InvalidReference, "duplicate_export"));
            }
        }
        if definition.exports.len() != selected.len() {
            return Err(error(ErrorCode::InvalidReference, "exports"));
        }
        for definition in &definition.exports {
            let AdapterExportDefinition::Tool {
                metadata,
                descriptor,
            } = definition
            else {
                return Err(error(ErrorCode::CapabilityUnsupported, "export_kind"));
            };
            let export = selected
                .get(&metadata.export_id)
                .ok_or_else(|| error(ErrorCode::InvalidReference, "export"))?;
            if descriptor.as_ref() != export.compiled.descriptor() {
                return Err(error(
                    ErrorCode::InvalidToolInputContract,
                    "export_descriptor",
                ));
            }
        }
        Ok(Self {
            definition,
            connection_name,
            command,
            limits,
            snapshot,
            exports: selected,
        })
    }
}
impl AdapterFactory for McpAdapterFactory {
    fn open<'a>(
        &'a self,
        context: &'a AdapterInitContext,
    ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
        Box::pin(async move {
            if context.execution.scope != *self.snapshot.scope() {
                return Err(error(ErrorCode::AccessDenied, "factory_scope"));
            }
            if context.binding.binding.adapter_id != self.definition.metadata.reference.id
                || Some(&context.binding.binding.version)
                    != self.definition.metadata.reference.version.as_ref()
                || context.binding.definition != self.definition
                || context.binding.definition_digest != self.definition.digest()
                || context
                    .binding
                    .connections
                    .get(&self.connection_name)
                    .map(|c| &c.connection_ref)
                    != Some(self.snapshot.connection_ref())
                || context
                    .binding
                    .binding
                    .config
                    .as_ref()
                    .is_some_and(|v| !v.is_empty())
            {
                return Err(error(ErrorCode::InvalidReference, "factory_binding"));
            }
            if context.execution.purpose == ComponentBindPurpose::ObserversOnly
                && !context.selected_exports.is_empty()
            {
                return Err(error(ErrorCode::AccessDenied, "observer_tools"));
            }
            let binding = context.binding.binding.binding_id.clone();
            let mut chosen = vec![];
            for selection in &context.selected_exports {
                if selection.adapter_binding != binding {
                    return Err(error(ErrorCode::InvalidReference, "selection"));
                }
                chosen.push(
                    self.exports
                        .get(&selection.export_id)
                        .ok_or_else(|| error(ErrorCode::InvalidReference, "selection"))?,
                );
            }
            let mut outputs = vec![];
            let mut client = None;
            if !chosen.is_empty() {
                let opened = McpClient::connect(
                    context.execution.scope.clone(),
                    self.snapshot.connection_ref().clone(),
                    self.command.clone(),
                    self.limits.clone(),
                    &context.execution.cancellation,
                    context.execution.deadline,
                )
                .await?;
                let current = opened
                    .discover(
                        &context.execution.scope,
                        &context.execution.cancellation,
                        context.execution.deadline,
                    )
                    .await?;
                if chosen
                    .iter()
                    .any(|e| !self.snapshot.matches_tool(&current, &e.remote_name))
                {
                    let _ = opened
                        .close(tokio::time::Instant::now() + self.limits.close_timeout)
                        .await;
                    return Err(error(
                        ErrorCode::InvalidToolInputContract,
                        "descriptor_drift",
                    ));
                }
                for export in chosen {
                    let mut executor = opened.bind_tool(
                        &self.snapshot,
                        &export.remote_name,
                        export.compiled.clone(),
                    )?;
                    executor.segment = Some((
                        context.execution.run_id.clone(),
                        context.execution.binding_set_id.clone(),
                    ));
                    outputs.push(AdapterExportInstance::Tool {
                        export_id: export.export_id.clone(),
                        descriptor: Box::new(export.compiled.descriptor().clone()),
                        executor: Arc::new(executor),
                    });
                }
                client = Some(opened);
            }
            Ok(Arc::new(Instance {
                scope: context.execution.scope.clone(),
                run: context.execution.run_id.clone(),
                segment: context.execution.binding_set_id.clone(),
                binding,
                client,
                outputs,
            }) as Arc<dyn AdapterInstance>)
        })
    }
}
struct Instance {
    scope: Scope,
    run: Id,
    segment: Id,
    binding: Id,
    client: Option<McpClient>,
    outputs: Vec<AdapterExportInstance>,
}
impl AdapterInstance for Instance {
    fn exports(&self) -> Vec<AdapterExportInstance> {
        self.outputs.clone()
    }
    fn close<'a>(&'a self, context: &'a AdapterCloseContext) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if context.scope != self.scope
                || context.run_id != self.run
                || context.binding_set_id != self.segment
                || context.adapter_binding != self.binding
            {
                return Err(error(ErrorCode::AccessDenied, "close_scope"));
            }
            if let Some(client) = &self.client {
                client
                    .close_with(context.deadline, &context.cancellation)
                    .await?;
            }
            Ok(())
        })
    }
}
