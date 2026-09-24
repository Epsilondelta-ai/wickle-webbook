# MCP tools over stdio

`wickle-mcp` connects a Host-approved local MCP server to Wickle's existing Tool contracts. The core has no MCP dependency. This adapter supports the **2025-06-18 tools/stdio subset**, using pinned `rmcp` 0.8.5 for Rust 1.85 compatibility. It does not claim support for every MCP feature or a newer protocol revision.

## Connect and approve

1. Construct `McpCommand` with an absolute executable path, literal arguments and an explicit environment. The adapter clears the inherited environment, does not invoke a shell, and discards child stderr. Supply credentials through the Host's protected connection configuration.
2. Call `McpClient::connect` with the owner `Scope`, a versioned connection reference, finite `McpLimits`, cancellation and deadline. Initialization requires the exact supported protocol and the server's tools capability.
3. Call `discover` for a bounded `McpSnapshot`. Store its serialized form and digest in protected Host storage. Raw names, schemas, server identity and metadata remain available for review. Discovery never activates tools.
4. Select one remote name and create `McpToolApproval` with a stable Tool ID, model-facing name and explicit `agent_parameters`. Classify effects yourself; remote annotations are hints, not authorization.
5. Obtain `snapshot.descriptor(...)`, then compile it with `SchemaCompiler` and your `SystemInputRegistry`. Register the resulting `McpToolExecutor` from `bind_tool`, or use `McpAdapterFactory` with the adapter runtime.

For a tool requiring `query`, `limit` and `workspace_id`, select only `query` and `limit` as agent parameters. Register the hidden UUID as a system input; optionally map its property name through `system_bindings`. The Agent first decodes model arguments using the frozen provider Tool contract. The core applies model defaults and validates the canonical model-owned arguments, then binds protected inputs and validates the full execution schema. A malformed encoding, invalid model argument or attempted system-field injection receives repair feedback before any remote call. MCP does not define a second provider-specific argument schema. The MCP executor receives only those final arguments, never the full system context. See [tool inputs](tool-inputs.md) for the shared contract.

The [standalone consumer](../tests/support/mcp_consumer.rs) demonstrates the low-level executor using a real local child process and final bound arguments. The MCP integration tests additionally exercise the full Agent loop, provider argument decoding, input defaults and system binding, stored results and duplicate-request suppression. They also check that a real remote write followed by connection loss or cancellation remains an unresolved effect without being replayed. Run `python3 scripts/check-package.py --allow-dirty` to build and execute the consumer from extracted Cargo archives; Python 3 is required for this test fixture, not for the library.

## Scope and lifecycle

`McpAdapterFactory` verifies the approved definition, version, connection reference and selected Tool exports. It opens a fresh scoped connection per run segment, returns original export descriptors for the runtime's alias attestation, and binds execution to that segment. An observer-only segment cannot activate Tools. No selected exports means no subprocess.

Before every call, the adapter rediscovers and checks the selected original descriptor and server identity. Changed descriptors fail before dispatch. Newly discovered tools are not automatically registered. A list-change notification during execution makes the result's effect unknown. Duplicate discovery responses cannot replace the first captured raw descriptor.

Calls are serialized. Cancellation, deadline failure or abandonment of an in-flight operation invalidates the session and starts tracked cleanup. `close` cancels the SDK service, kills and reaps the direct child, and awaits shutdown within bounds. Repeated callers await the same cleanup result. A close timeout is an error. The Host must keep the Tokio runtime alive for asynchronous cleanup; descendant-process isolation belongs to the Host's process sandbox.

## Effects and output

MCP does not supply a trusted Wickle effect receipt. Calls are never retried automatically and reconciliation is not advertised. A Host-classified read-only tool reports `NotApplied`; a write or unknown tool reports `Unknown`, including a successful response. Use a Host verifier or wrapper if your application needs durable write attestation.

Declared structured output is required when `outputSchema` is present and is validated by the core. Otherwise only text content is accepted, with unsupported content rejected and metadata stripped from model output. Frame, message, page, tool-count and output limits are finite. Remote diagnostics become stable safe failure codes rather than raw error messages.

Sampling/model execution, prompts, resources, elicitation, HTTP transport, OAuth discovery and automatic permission assignment are outside this adapter. The client advertises no sampling capability and does not fulfill server-initiated model requests.
