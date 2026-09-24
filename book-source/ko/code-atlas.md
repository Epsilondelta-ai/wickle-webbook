# 0.2.0 전체 코드 지도

[목차](README.md) · [변경 지도](changes-v0.2.md)

최종 태그 파일 367개 중 Rust 파일은 268개다. 소스 링크는 동봉된 0.2.0 reference를 가리킨다. 최초/마지막 변경 장은 그 단계의 전체 구현 문서로 연결된다. 기초 단계의 코드와 최종 코드가 다를 수 있다.

| 최종 Rust 파일 | 최초 단계 | 마지막 변경 |
| --- | --- | --- |
| [crates/wickle-adapter-runtime/src/lib.rs](../reference/crates/wickle-adapter-runtime/src/lib.rs) | [19](implementation/19-adapters.md) | [20](implementation/20-sources.md) |
| [crates/wickle-adapter-runtime/src/lifecycle.rs](../reference/crates/wickle-adapter-runtime/src/lifecycle.rs) | [19](implementation/19-adapters.md) | [45](implementation/45-fragments.md) |
| [crates/wickle-adapter-runtime/src/registry.rs](../reference/crates/wickle-adapter-runtime/src/registry.rs) | [19](implementation/19-adapters.md) | [20](implementation/20-sources.md) |
| [crates/wickle-adapter-runtime/src/runtime.rs](../reference/crates/wickle-adapter-runtime/src/runtime.rs) | [19](implementation/19-adapters.md) | [20](implementation/20-sources.md) |
| [crates/wickle-adapter-runtime/tests/agent_components.rs](../reference/crates/wickle-adapter-runtime/tests/agent_components.rs) | [19](implementation/19-adapters.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-adapter-runtime/tests/context_sources.rs](../reference/crates/wickle-adapter-runtime/tests/context_sources.rs) | [20](implementation/20-sources.md) | [48](implementation/48-controls.md) |
| [crates/wickle-adapter-runtime/tests/hook_exports.rs](../reference/crates/wickle-adapter-runtime/tests/hook_exports.rs) | [19](implementation/19-adapters.md) | [19](implementation/19-adapters.md) |
| [crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs](../reference/crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs) | [19](implementation/19-adapters.md) | [24](implementation/24-recovery.md) |
| [crates/wickle-adapter-runtime/tests/runtime_registry.rs](../reference/crates/wickle-adapter-runtime/tests/runtime_registry.rs) | [19](implementation/19-adapters.md) | [20](implementation/20-sources.md) |
| [crates/wickle-adapter-runtime/tests/support/context_sources.rs](../reference/crates/wickle-adapter-runtime/tests/support/context_sources.rs) | [20](implementation/20-sources.md) | [45](implementation/45-fragments.md) |
| [crates/wickle-adapter-runtime/tests/support/mod.rs](../reference/crates/wickle-adapter-runtime/tests/support/mod.rs) | [19](implementation/19-adapters.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle-mcp/src/client.rs](../reference/crates/wickle-mcp/src/client.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/src/executor.rs](../reference/crates/wickle-mcp/src/executor.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/src/factory.rs](../reference/crates/wickle-mcp/src/factory.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/src/lib.rs](../reference/crates/wickle-mcp/src/lib.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/src/snapshot.rs](../reference/crates/wickle-mcp/src/snapshot.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/src/transport.rs](../reference/crates/wickle-mcp/src/transport.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/tests/agent_contract.rs](../reference/crates/wickle-mcp/tests/agent_contract.rs) | [57](implementation/57-mcp-repair.md) | [57](implementation/57-mcp-repair.md) |
| [crates/wickle-mcp/tests/factory.rs](../reference/crates/wickle-mcp/tests/factory.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-mcp/tests/stdio.rs](../reference/crates/wickle-mcp/tests/stdio.rs) | [32](implementation/32-mcp.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle-mcp/tests/support/binding.rs](../reference/crates/wickle-mcp/tests/support/binding.rs) | [32](implementation/32-mcp.md) | [48](implementation/48-controls.md) |
| [crates/wickle-mcp/tests/support/mod.rs](../reference/crates/wickle-mcp/tests/support/mod.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [crates/wickle-model-anthropic/src/codec.rs](../reference/crates/wickle-model-anthropic/src/codec.rs) | [27](implementation/27-anthropic.md) | [52](implementation/52-anthropic-repair.md) |
| [crates/wickle-model-anthropic/src/connection.rs](../reference/crates/wickle-model-anthropic/src/connection.rs) | [27](implementation/27-anthropic.md) | [27](implementation/27-anthropic.md) |
| [crates/wickle-model-anthropic/src/inspection.rs](../reference/crates/wickle-model-anthropic/src/inspection.rs) | [27](implementation/27-anthropic.md) | [27](implementation/27-anthropic.md) |
| [crates/wickle-model-anthropic/src/lib.rs](../reference/crates/wickle-model-anthropic/src/lib.rs) | [27](implementation/27-anthropic.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-anthropic/src/model.rs](../reference/crates/wickle-model-anthropic/src/model.rs) | [27](implementation/27-anthropic.md) | [27](implementation/27-anthropic.md) |
| [crates/wickle-model-anthropic/src/response.rs](../reference/crates/wickle-model-anthropic/src/response.rs) | [27](implementation/27-anthropic.md) | [52](implementation/52-anthropic-repair.md) |
| [crates/wickle-model-anthropic/tests/agent_contract.rs](../reference/crates/wickle-model-anthropic/tests/agent_contract.rs) | [52](implementation/52-anthropic-repair.md) | [52](implementation/52-anthropic-repair.md) |
| [crates/wickle-model-anthropic/tests/messages.rs](../reference/crates/wickle-model-anthropic/tests/messages.rs) | [27](implementation/27-anthropic.md) | [52](implementation/52-anthropic-repair.md) |
| [crates/wickle-model-anthropic/tests/support/mod.rs](../reference/crates/wickle-model-anthropic/tests/support/mod.rs) | [27](implementation/27-anthropic.md) | [27](implementation/27-anthropic.md) |
| [crates/wickle-model-azure-openai/src/auth.rs](../reference/crates/wickle-model-azure-openai/src/auth.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-azure-openai/src/connection.rs](../reference/crates/wickle-model-azure-openai/src/connection.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-azure-openai/src/inspection.rs](../reference/crates/wickle-model-azure-openai/src/inspection.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-azure-openai/src/lib.rs](../reference/crates/wickle-model-azure-openai/src/lib.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-azure-openai/src/model.rs](../reference/crates/wickle-model-azure-openai/src/model.rs) | [26](implementation/26-azure.md) | [51](implementation/51-azure-schema.md) |
| [crates/wickle-model-azure-openai/tests/agent_contract.rs](../reference/crates/wickle-model-azure-openai/tests/agent_contract.rs) | [51](implementation/51-azure-schema.md) | [51](implementation/51-azure-schema.md) |
| [crates/wickle-model-azure-openai/tests/responses.rs](../reference/crates/wickle-model-azure-openai/tests/responses.rs) | [26](implementation/26-azure.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-model-azure-openai/tests/support/mod.rs](../reference/crates/wickle-model-azure-openai/tests/support/mod.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-bedrock/src/auth.rs](../reference/crates/wickle-model-bedrock/src/auth.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-bedrock/src/connection.rs](../reference/crates/wickle-model-bedrock/src/connection.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-bedrock/src/framing.rs](../reference/crates/wickle-model-bedrock/src/framing.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-bedrock/src/inspection.rs](../reference/crates/wickle-model-bedrock/src/inspection.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-bedrock/src/lib.rs](../reference/crates/wickle-model-bedrock/src/lib.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [crates/wickle-model-bedrock/src/model.rs](../reference/crates/wickle-model-bedrock/src/model.rs) | [28](implementation/28-bedrock.md) | [53](implementation/53-bedrock-repair.md) |
| [crates/wickle-model-bedrock/tests/agent_contract.rs](../reference/crates/wickle-model-bedrock/tests/agent_contract.rs) | [53](implementation/53-bedrock-repair.md) | [53](implementation/53-bedrock-repair.md) |
| [crates/wickle-model-bedrock/tests/bedrock.rs](../reference/crates/wickle-model-bedrock/tests/bedrock.rs) | [28](implementation/28-bedrock.md) | [53](implementation/53-bedrock-repair.md) |
| [crates/wickle-model-bedrock/tests/support/mod.rs](../reference/crates/wickle-model-bedrock/tests/support/mod.rs) | [28](implementation/28-bedrock.md) | [53](implementation/53-bedrock-repair.md) |
| [crates/wickle-model-gemini/src/codec.rs](../reference/crates/wickle-model-gemini/src/codec.rs) | [29](implementation/29-gemini.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/src/connection.rs](../reference/crates/wickle-model-gemini/src/connection.rs) | [29](implementation/29-gemini.md) | [29](implementation/29-gemini.md) |
| [crates/wickle-model-gemini/src/inspection.rs](../reference/crates/wickle-model-gemini/src/inspection.rs) | [29](implementation/29-gemini.md) | [29](implementation/29-gemini.md) |
| [crates/wickle-model-gemini/src/lib.rs](../reference/crates/wickle-model-gemini/src/lib.rs) | [29](implementation/29-gemini.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/src/model.rs](../reference/crates/wickle-model-gemini/src/model.rs) | [29](implementation/29-gemini.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/src/raw.rs](../reference/crates/wickle-model-gemini/src/raw.rs) | [54](implementation/54-gemini-schema.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/src/response.rs](../reference/crates/wickle-model-gemini/src/response.rs) | [29](implementation/29-gemini.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/src/schema.rs](../reference/crates/wickle-model-gemini/src/schema.rs) | [54](implementation/54-gemini-schema.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/tests/agent_contract.rs](../reference/crates/wickle-model-gemini/tests/agent_contract.rs) | [54](implementation/54-gemini-schema.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/tests/generate.rs](../reference/crates/wickle-model-gemini/tests/generate.rs) | [29](implementation/29-gemini.md) | [54](implementation/54-gemini-schema.md) |
| [crates/wickle-model-gemini/tests/support/mod.rs](../reference/crates/wickle-model-gemini/tests/support/mod.rs) | [29](implementation/29-gemini.md) | [29](implementation/29-gemini.md) |
| [crates/wickle-model-openai/src/connection.rs](../reference/crates/wickle-model-openai/src/connection.rs) | [25](implementation/25-openai.md) | [25](implementation/25-openai.md) |
| [crates/wickle-model-openai/src/inspection.rs](../reference/crates/wickle-model-openai/src/inspection.rs) | [25](implementation/25-openai.md) | [25](implementation/25-openai.md) |
| [crates/wickle-model-openai/src/lib.rs](../reference/crates/wickle-model-openai/src/lib.rs) | [25](implementation/25-openai.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-openai/src/model.rs](../reference/crates/wickle-model-openai/src/model.rs) | [25](implementation/25-openai.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle-model-openai/tests/agent_contract.rs](../reference/crates/wickle-model-openai/tests/agent_contract.rs) | [50](implementation/50-openai-schema.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle-model-openai/tests/responses.rs](../reference/crates/wickle-model-openai/tests/responses.rs) | [25](implementation/25-openai.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-model-openai/tests/support/mod.rs](../reference/crates/wickle-model-openai/tests/support/mod.rs) | [25](implementation/25-openai.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-responses/src/codec.rs](../reference/crates/wickle-model-responses/src/codec.rs) | [26](implementation/26-azure.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-responses/src/lib.rs](../reference/crates/wickle-model-responses/src/lib.rs) | [26](implementation/26-azure.md) | [51](implementation/51-azure-schema.md) |
| [crates/wickle-model-responses/src/response.rs](../reference/crates/wickle-model-responses/src/response.rs) | [26](implementation/26-azure.md) | [31](implementation/31-xai.md) |
| [crates/wickle-model-responses/src/schema.rs](../reference/crates/wickle-model-responses/src/schema.rs) | [50](implementation/50-openai-schema.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-model-responses/src/sse.rs](../reference/crates/wickle-model-responses/src/sse.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [crates/wickle-model-router/src/dispatcher.rs](../reference/crates/wickle-model-router/src/dispatcher.rs) | [14](implementation/14-routing.md) | [14](implementation/14-routing.md) |
| [crates/wickle-model-router/src/lib.rs](../reference/crates/wickle-model-router/src/lib.rs) | [12](implementation/12-catalog.md) | [14](implementation/14-routing.md) |
| [crates/wickle-model-router/src/routing.rs](../reference/crates/wickle-model-router/src/routing.rs) | [14](implementation/14-routing.md) | [14](implementation/14-routing.md) |
| [crates/wickle-model-router/tests/catalog.rs](../reference/crates/wickle-model-router/tests/catalog.rs) | [12](implementation/12-catalog.md) | [42](implementation/42-options.md) |
| [crates/wickle-model-router/tests/dispatcher.rs](../reference/crates/wickle-model-router/tests/dispatcher.rs) | [14](implementation/14-routing.md) | [14](implementation/14-routing.md) |
| [crates/wickle-model-router/tests/routed_execution.rs](../reference/crates/wickle-model-router/tests/routed_execution.rs) | [14](implementation/14-routing.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle-model-router/tests/routing.rs](../reference/crates/wickle-model-router/tests/routing.rs) | [14](implementation/14-routing.md) | [42](implementation/42-options.md) |
| [crates/wickle-model-router/tests/support/routed.rs](../reference/crates/wickle-model-router/tests/support/routed.rs) | [14](implementation/14-routing.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle-model-vertex/src/auth.rs](../reference/crates/wickle-model-vertex/src/auth.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [crates/wickle-model-vertex/src/connection.rs](../reference/crates/wickle-model-vertex/src/connection.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [crates/wickle-model-vertex/src/inspection.rs](../reference/crates/wickle-model-vertex/src/inspection.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [crates/wickle-model-vertex/src/lib.rs](../reference/crates/wickle-model-vertex/src/lib.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [crates/wickle-model-vertex/src/model.rs](../reference/crates/wickle-model-vertex/src/model.rs) | [30](implementation/30-vertex.md) | [55](implementation/55-vertex-schema.md) |
| [crates/wickle-model-vertex/tests/agent_contract.rs](../reference/crates/wickle-model-vertex/tests/agent_contract.rs) | [55](implementation/55-vertex-schema.md) | [55](implementation/55-vertex-schema.md) |
| [crates/wickle-model-vertex/tests/support/mod.rs](../reference/crates/wickle-model-vertex/tests/support/mod.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [crates/wickle-model-vertex/tests/vertex.rs](../reference/crates/wickle-model-vertex/tests/vertex.rs) | [30](implementation/30-vertex.md) | [34](implementation/34-evidence.md) |
| [crates/wickle-model-xai/src/connection.rs](../reference/crates/wickle-model-xai/src/connection.rs) | [31](implementation/31-xai.md) | [31](implementation/31-xai.md) |
| [crates/wickle-model-xai/src/inspection.rs](../reference/crates/wickle-model-xai/src/inspection.rs) | [31](implementation/31-xai.md) | [31](implementation/31-xai.md) |
| [crates/wickle-model-xai/src/lib.rs](../reference/crates/wickle-model-xai/src/lib.rs) | [31](implementation/31-xai.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-xai/src/model.rs](../reference/crates/wickle-model-xai/src/model.rs) | [31](implementation/31-xai.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-xai/src/schema.rs](../reference/crates/wickle-model-xai/src/schema.rs) | [56](implementation/56-xai-contract.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-xai/tests/agent_contract.rs](../reference/crates/wickle-model-xai/tests/agent_contract.rs) | [56](implementation/56-xai-contract.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-xai/tests/responses.rs](../reference/crates/wickle-model-xai/tests/responses.rs) | [31](implementation/31-xai.md) | [34](implementation/34-evidence.md) |
| [crates/wickle-model-xai/tests/schema.rs](../reference/crates/wickle-model-xai/tests/schema.rs) | [56](implementation/56-xai-contract.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle-model-xai/tests/support/mod.rs](../reference/crates/wickle-model-xai/tests/support/mod.rs) | [31](implementation/31-xai.md) | [31](implementation/31-xai.md) |
| [crates/wickle-state-sqlite/src/lib.rs](../reference/crates/wickle-state-sqlite/src/lib.rs) | [13](implementation/13-sqlite.md) | [40](implementation/40-atomic-store.md) |
| [crates/wickle-state-sqlite/tests/agent_recovery.rs](../reference/crates/wickle-state-sqlite/tests/agent_recovery.rs) | [24](implementation/24-recovery.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/execution_store.rs](../reference/crates/wickle-state-sqlite/tests/execution_store.rs) | [40](implementation/40-atomic-store.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/host_contract.rs](../reference/crates/wickle-state-sqlite/tests/host_contract.rs) | [33](implementation/33-events.md) | [33](implementation/33-events.md) |
| [crates/wickle-state-sqlite/tests/state_store.rs](../reference/crates/wickle-state-sqlite/tests/state_store.rs) | [13](implementation/13-sqlite.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/support/command_recovery.rs](../reference/crates/wickle-state-sqlite/tests/support/command_recovery.rs) | [58](implementation/58-recovery-audit.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/support/context_recovery.rs](../reference/crates/wickle-state-sqlite/tests/support/context_recovery.rs) | [24](implementation/24-recovery.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/support/mod.rs](../reference/crates/wickle-state-sqlite/tests/support/mod.rs) | [13](implementation/13-sqlite.md) | [45](implementation/45-fragments.md) |
| [crates/wickle-state-sqlite/tests/support/recovery_store.rs](../reference/crates/wickle-state-sqlite/tests/support/recovery_store.rs) | [24](implementation/24-recovery.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle-state-sqlite/tests/support/workers.rs](../reference/crates/wickle-state-sqlite/tests/support/workers.rs) | [13](implementation/13-sqlite.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/src/agent.rs](../reference/crates/wickle/src/agent.rs) | [15](implementation/15-agent.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/src/agent/admission.rs](../reference/crates/wickle/src/agent/admission.rs) | [15](implementation/15-agent.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/agent/artifacts.rs](../reference/crates/wickle/src/agent/artifacts.rs) | [21](implementation/21-skills.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/agent/components.rs](../reference/crates/wickle/src/agent/components.rs) | [19](implementation/19-adapters.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/agent/control.rs](../reference/crates/wickle/src/agent/control.rs) | [48](implementation/48-controls.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/agent/driver.rs](../reference/crates/wickle/src/agent/driver.rs) | [15](implementation/15-agent.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/agent/hooks.rs](../reference/crates/wickle/src/agent/hooks.rs) | [18](implementation/18-hooks.md) | [19](implementation/19-adapters.md) |
| [crates/wickle/src/agent/inspection.rs](../reference/crates/wickle/src/agent/inspection.rs) | [49](implementation/49-inspection.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/src/agent/interruption.rs](../reference/crates/wickle/src/agent/interruption.rs) | [47](implementation/47-interruptions.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/agent/persistence.rs](../reference/crates/wickle/src/agent/persistence.rs) | [24](implementation/24-recovery.md) | [40](implementation/40-atomic-store.md) |
| [crates/wickle/src/agent/recovery.rs](../reference/crates/wickle/src/agent/recovery.rs) | [24](implementation/24-recovery.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/agent/resume.rs](../reference/crates/wickle/src/agent/resume.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/agent/segment.rs](../reference/crates/wickle/src/agent/segment.rs) | [48](implementation/48-controls.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/agent/sources.rs](../reference/crates/wickle/src/agent/sources.rs) | [20](implementation/20-sources.md) | [20](implementation/20-sources.md) |
| [crates/wickle/src/agent/tools.rs](../reference/crates/wickle/src/agent/tools.rs) | [16](implementation/16-tools.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/agent/verification.rs](../reference/crates/wickle/src/agent/verification.rs) | [23](implementation/23-verification.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/artifacts.rs](../reference/crates/wickle/src/artifacts.rs) | [21](implementation/21-skills.md) | [22](implementation/22-compaction.md) |
| [crates/wickle/src/budget.rs](../reference/crates/wickle/src/budget.rs) | [7](implementation/07-budget.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/canonical.rs](../reference/crates/wickle/src/canonical.rs) | [38](implementation/38-canonical-json.md) | [38](implementation/38-canonical-json.md) |
| [crates/wickle/src/clock.rs](../reference/crates/wickle/src/clock.rs) | [7](implementation/07-budget.md) | [7](implementation/07-budget.md) |
| [crates/wickle/src/component_runtime.rs](../reference/crates/wickle/src/component_runtime.rs) | [19](implementation/19-adapters.md) | [20](implementation/20-sources.md) |
| [crates/wickle/src/context.rs](../reference/crates/wickle/src/context.rs) | [4](implementation/04-contracts.md) | [4](implementation/04-contracts.md) |
| [crates/wickle/src/context_fragment.rs](../reference/crates/wickle/src/context_fragment.rs) | [45](implementation/45-fragments.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_lineage.rs](../reference/crates/wickle/src/context_lineage.rs) | [45](implementation/45-fragments.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_projection.rs](../reference/crates/wickle/src/context_projection.rs) | [10](implementation/10-context.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/context_source.rs](../reference/crates/wickle/src/context_source.rs) | [20](implementation/20-sources.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_source/records.rs](../reference/crates/wickle/src/context_source/records.rs) | [20](implementation/20-sources.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_source/runtime.rs](../reference/crates/wickle/src/context_source/runtime.rs) | [20](implementation/20-sources.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/context_strategy.rs](../reference/crates/wickle/src/context_strategy.rs) | [22](implementation/22-compaction.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_strategy/compression.rs](../reference/crates/wickle/src/context_strategy/compression.rs) | [22](implementation/22-compaction.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_strategy/engine.rs](../reference/crates/wickle/src/context_strategy/engine.rs) | [22](implementation/22-compaction.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_strategy/model_compactor.rs](../reference/crates/wickle/src/context_strategy/model_compactor.rs) | [22](implementation/22-compaction.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/context_strategy/operations.rs](../reference/crates/wickle/src/context_strategy/operations.rs) | [22](implementation/22-compaction.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/context_strategy/records.rs](../reference/crates/wickle/src/context_strategy/records.rs) | [22](implementation/22-compaction.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/context_strategy/runtime.rs](../reference/crates/wickle/src/context_strategy/runtime.rs) | [22](implementation/22-compaction.md) | [41](implementation/41-admission.md) |
| [crates/wickle/src/error.rs](../reference/crates/wickle/src/error.rs) | [4](implementation/04-contracts.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/src/execution_contracts.rs](../reference/crates/wickle/src/execution_contracts.rs) | [39](implementation/39-execution-contracts.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/future.rs](../reference/crates/wickle/src/future.rs) | [23](implementation/23-verification.md) | [23](implementation/23-verification.md) |
| [crates/wickle/src/hooks.rs](../reference/crates/wickle/src/hooks.rs) | [18](implementation/18-hooks.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/src/hooks/records.rs](../reference/crates/wickle/src/hooks/records.rs) | [18](implementation/18-hooks.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/src/hooks/runtime.rs](../reference/crates/wickle/src/hooks/runtime.rs) | [18](implementation/18-hooks.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/input_binding.rs](../reference/crates/wickle/src/input_binding.rs) | [11](implementation/11-binding.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/inspection.rs](../reference/crates/wickle/src/inspection.rs) | [49](implementation/49-inspection.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/src/interruption.rs](../reference/crates/wickle/src/interruption.rs) | [47](implementation/47-interruptions.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/lib.rs](../reference/crates/wickle/src/lib.rs) | [3](implementation/03-workspace.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/message.rs](../reference/crates/wickle/src/message.rs) | [4](implementation/04-contracts.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/model.rs](../reference/crates/wickle/src/model.rs) | [4](implementation/04-contracts.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/model_catalog.rs](../reference/crates/wickle/src/model_catalog.rs) | [12](implementation/12-catalog.md) | [42](implementation/42-options.md) |
| [crates/wickle/src/model_dispatch.rs](../reference/crates/wickle/src/model_dispatch.rs) | [14](implementation/14-routing.md) | [14](implementation/14-routing.md) |
| [crates/wickle/src/model_execution.rs](../reference/crates/wickle/src/model_execution.rs) | [8](implementation/08-model.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/model_execution/prepared.rs](../reference/crates/wickle/src/model_execution/prepared.rs) | [46](implementation/46-prepared-step.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/model_execution/routed.rs](../reference/crates/wickle/src/model_execution/routed.rs) | [14](implementation/14-routing.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/model_options.rs](../reference/crates/wickle/src/model_options.rs) | [42](implementation/42-options.md) | [42](implementation/42-options.md) |
| [crates/wickle/src/model_protocol.rs](../reference/crates/wickle/src/model_protocol.rs) | [8](implementation/08-model.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/model_routing.rs](../reference/crates/wickle/src/model_routing.rs) | [14](implementation/14-routing.md) | [42](implementation/42-options.md) |
| [crates/wickle/src/policy.rs](../reference/crates/wickle/src/policy.rs) | [5](implementation/05-policy.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/src/prepared_step.rs](../reference/crates/wickle/src/prepared_step.rs) | [46](implementation/46-prepared-step.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/profile.rs](../reference/crates/wickle/src/profile.rs) | [4](implementation/04-contracts.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/src/provider_tool_schema.rs](../reference/crates/wickle/src/provider_tool_schema.rs) | [43](implementation/43-provider-contracts.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle/src/recovery.rs](../reference/crates/wickle/src/recovery.rs) | [24](implementation/24-recovery.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/resolution.rs](../reference/crates/wickle/src/resolution.rs) | [4](implementation/04-contracts.md) | [20](implementation/20-sources.md) |
| [crates/wickle/src/run.rs](../reference/crates/wickle/src/run.rs) | [4](implementation/04-contracts.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/serialization.rs](../reference/crates/wickle/src/serialization.rs) | [4](implementation/04-contracts.md) | [38](implementation/38-canonical-json.md) |
| [crates/wickle/src/skills.rs](../reference/crates/wickle/src/skills.rs) | [21](implementation/21-skills.md) | [21](implementation/21-skills.md) |
| [crates/wickle/src/skills/records.rs](../reference/crates/wickle/src/skills/records.rs) | [21](implementation/21-skills.md) | [21](implementation/21-skills.md) |
| [crates/wickle/src/skills/runtime.rs](../reference/crates/wickle/src/skills/runtime.rs) | [21](implementation/21-skills.md) | [21](implementation/21-skills.md) |
| [crates/wickle/src/state.rs](../reference/crates/wickle/src/state.rs) | [6](implementation/06-state.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/state/checkpoint.rs](../reference/crates/wickle/src/state/checkpoint.rs) | [13](implementation/13-sqlite.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/state/context_state.rs](../reference/crates/wickle/src/state/context_state.rs) | [22](implementation/22-compaction.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/src/state/execution.rs](../reference/crates/wickle/src/state/execution.rs) | [40](implementation/40-atomic-store.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/state/hook_state.rs](../reference/crates/wickle/src/state/hook_state.rs) | [18](implementation/18-hooks.md) | [22](implementation/22-compaction.md) |
| [crates/wickle/src/state/interruption_state.rs](../reference/crates/wickle/src/state/interruption_state.rs) | [47](implementation/47-interruptions.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/state/prepared_state.rs](../reference/crates/wickle/src/state/prepared_state.rs) | [46](implementation/46-prepared-step.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/src/state/reconciliation_state.rs](../reference/crates/wickle/src/state/reconciliation_state.rs) | [24](implementation/24-recovery.md) | [24](implementation/24-recovery.md) |
| [crates/wickle/src/state/recovery_state.rs](../reference/crates/wickle/src/state/recovery_state.rs) | [24](implementation/24-recovery.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/src/state/skill_state.rs](../reference/crates/wickle/src/state/skill_state.rs) | [21](implementation/21-skills.md) | [21](implementation/21-skills.md) |
| [crates/wickle/src/state/source_state.rs](../reference/crates/wickle/src/state/source_state.rs) | [20](implementation/20-sources.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/state/verification_state.rs](../reference/crates/wickle/src/state/verification_state.rs) | [23](implementation/23-verification.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/src/tool_execution.rs](../reference/crates/wickle/src/tool_execution.rs) | [16](implementation/16-tools.md) | [24](implementation/24-recovery.md) |
| [crates/wickle/src/tool_execution/reconciliation.rs](../reference/crates/wickle/src/tool_execution/reconciliation.rs) | [24](implementation/24-recovery.md) | [24](implementation/24-recovery.md) |
| [crates/wickle/src/tool_execution/resume.rs](../reference/crates/wickle/src/tool_execution/resume.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/tool_execution/round.rs](../reference/crates/wickle/src/tool_execution/round.rs) | [16](implementation/16-tools.md) | [48](implementation/48-controls.md) |
| [crates/wickle/src/tool_schema.rs](../reference/crates/wickle/src/tool_schema.rs) | [9](implementation/09-schema.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/src/verification.rs](../reference/crates/wickle/src/verification.rs) | [23](implementation/23-verification.md) | [42](implementation/42-options.md) |
| [crates/wickle/src/views.rs](../reference/crates/wickle/src/views.rs) | [5](implementation/05-policy.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/agent_control.rs](../reference/crates/wickle/tests/agent_control.rs) | [48](implementation/48-controls.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/agent_hooks.rs](../reference/crates/wickle/tests/agent_hooks.rs) | [18](implementation/18-hooks.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/tests/agent_inspection.rs](../reference/crates/wickle/tests/agent_inspection.rs) | [49](implementation/49-inspection.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/tests/agent_interruption.rs](../reference/crates/wickle/tests/agent_interruption.rs) | [47](implementation/47-interruptions.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/tests/agent_recovery.rs](../reference/crates/wickle/tests/agent_recovery.rs) | [24](implementation/24-recovery.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/agent_resume.rs](../reference/crates/wickle/tests/agent_resume.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/agent_runtime.rs](../reference/crates/wickle/tests/agent_runtime.rs) | [15](implementation/15-agent.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/agent_tool_loop.rs](../reference/crates/wickle/tests/agent_tool_loop.rs) | [16](implementation/16-tools.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/artifacts.rs](../reference/crates/wickle/tests/artifacts.rs) | [21](implementation/21-skills.md) | [21](implementation/21-skills.md) |
| [crates/wickle/tests/budget.rs](../reference/crates/wickle/tests/budget.rs) | [7](implementation/07-budget.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/tests/context_projection.rs](../reference/crates/wickle/tests/context_projection.rs) | [10](implementation/10-context.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/context_sources.rs](../reference/crates/wickle/tests/context_sources.rs) | [20](implementation/20-sources.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/tests/contracts.rs](../reference/crates/wickle/tests/contracts.rs) | [4](implementation/04-contracts.md) | [47](implementation/47-interruptions.md) |
| [crates/wickle/tests/execution_contracts.rs](../reference/crates/wickle/tests/execution_contracts.rs) | [39](implementation/39-execution-contracts.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/execution_store.rs](../reference/crates/wickle/tests/execution_store.rs) | [40](implementation/40-atomic-store.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/input_binding.rs](../reference/crates/wickle/tests/input_binding.rs) | [11](implementation/11-binding.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/tests/model_execution.rs](../reference/crates/wickle/tests/model_execution.rs) | [8](implementation/08-model.md) | [50](implementation/50-openai-schema.md) |
| [crates/wickle/tests/model_protocol.rs](../reference/crates/wickle/tests/model_protocol.rs) | [8](implementation/08-model.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/tests/policy.rs](../reference/crates/wickle/tests/policy.rs) | [5](implementation/05-policy.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/provider_tool_schema.rs](../reference/crates/wickle/tests/provider_tool_schema.rs) | [43](implementation/43-provider-contracts.md) | [56](implementation/56-xai-contract.md) |
| [crates/wickle/tests/skills.rs](../reference/crates/wickle/tests/skills.rs) | [21](implementation/21-skills.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/tests/state.rs](../reference/crates/wickle/tests/state.rs) | [6](implementation/06-state.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/state_checkpoint.rs](../reference/crates/wickle/tests/state_checkpoint.rs) | [13](implementation/13-sqlite.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/state_hooks.rs](../reference/crates/wickle/tests/state_hooks.rs) | [18](implementation/18-hooks.md) | [18](implementation/18-hooks.md) |
| [crates/wickle/tests/state_resume_invariants.rs](../reference/crates/wickle/tests/state_resume_invariants.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/state_sources.rs](../reference/crates/wickle/tests/state_sources.rs) | [20](implementation/20-sources.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/support/agent.rs](../reference/crates/wickle/tests/support/agent.rs) | [15](implementation/15-agent.md) | [49](implementation/49-inspection.md) |
| [crates/wickle/tests/support/agent_hooks.rs](../reference/crates/wickle/tests/support/agent_hooks.rs) | [18](implementation/18-hooks.md) | [44](implementation/44-argument-repair.md) |
| [crates/wickle/tests/support/agent_resume.rs](../reference/crates/wickle/tests/support/agent_resume.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/support/context_sources.rs](../reference/crates/wickle/tests/support/context_sources.rs) | [20](implementation/20-sources.md) | [46](implementation/46-prepared-step.md) |
| [crates/wickle/tests/support/execution_store.rs](../reference/crates/wickle/tests/support/execution_store.rs) | [40](implementation/40-atomic-store.md) | [58](implementation/58-recovery-audit.md) |
| [crates/wickle/tests/support/mod.rs](../reference/crates/wickle/tests/support/mod.rs) | [7](implementation/07-budget.md) | [48](implementation/48-controls.md) |
| [crates/wickle/tests/support/tool_execution.rs](../reference/crates/wickle/tests/support/tool_execution.rs) | [16](implementation/16-tools.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/tests/tool_execution.rs](../reference/crates/wickle/tests/tool_execution.rs) | [16](implementation/16-tools.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/tests/tool_schema.rs](../reference/crates/wickle/tests/tool_schema.rs) | [9](implementation/09-schema.md) | [43](implementation/43-provider-contracts.md) |
| [crates/wickle/tests/verification.rs](../reference/crates/wickle/tests/verification.rs) | [23](implementation/23-verification.md) | [45](implementation/45-fragments.md) |
| [crates/wickle/tests/versioned_json.rs](../reference/crates/wickle/tests/versioned_json.rs) | [38](implementation/38-canonical-json.md) | [38](implementation/38-canonical-json.md) |
| [tests/host_contract/delivery.rs](../reference/tests/host_contract/delivery.rs) | [33](implementation/33-events.md) | [33](implementation/33-events.md) |
| [tests/support/adapter_consumer.rs](../reference/tests/support/adapter_consumer.rs) | [19](implementation/19-adapters.md) | [48](implementation/48-controls.md) |
| [tests/support/agent_consumer.rs](../reference/tests/support/agent_consumer.rs) | [15](implementation/15-agent.md) | [59](implementation/59-host-migration.md) |
| [tests/support/anthropic_consumer.rs](../reference/tests/support/anthropic_consumer.rs) | [27](implementation/27-anthropic.md) | [27](implementation/27-anthropic.md) |
| [tests/support/azure_consumer.rs](../reference/tests/support/azure_consumer.rs) | [26](implementation/26-azure.md) | [26](implementation/26-azure.md) |
| [tests/support/bedrock_consumer.rs](../reference/tests/support/bedrock_consumer.rs) | [28](implementation/28-bedrock.md) | [28](implementation/28-bedrock.md) |
| [tests/support/budget_consumer.rs](../reference/tests/support/budget_consumer.rs) | [7](implementation/07-budget.md) | [48](implementation/48-controls.md) |
| [tests/support/canonical_consumer.rs](../reference/tests/support/canonical_consumer.rs) | [38](implementation/38-canonical-json.md) | [38](implementation/38-canonical-json.md) |
| [tests/support/catalog_consumer.rs](../reference/tests/support/catalog_consumer.rs) | [12](implementation/12-catalog.md) | [42](implementation/42-options.md) |
| [tests/support/compaction_consumer.rs](../reference/tests/support/compaction_consumer.rs) | [22](implementation/22-compaction.md) | [47](implementation/47-interruptions.md) |
| [tests/support/consumer.rs](../reference/tests/support/consumer.rs) | [3](implementation/03-workspace.md) | [4](implementation/04-contracts.md) |
| [tests/support/context_consumer.rs](../reference/tests/support/context_consumer.rs) | [10](implementation/10-context.md) | [48](implementation/48-controls.md) |
| [tests/support/event_consumer.rs](../reference/tests/support/event_consumer.rs) | [33](implementation/33-events.md) | [47](implementation/47-interruptions.md) |
| [tests/support/execution_contract_consumer.rs](../reference/tests/support/execution_contract_consumer.rs) | [39](implementation/39-execution-contracts.md) | [39](implementation/39-execution-contracts.md) |
| [tests/support/gather_consumer.rs](../reference/tests/support/gather_consumer.rs) | [35](implementation/35-integration.md) | [47](implementation/47-interruptions.md) |
| [tests/support/gemini_consumer.rs](../reference/tests/support/gemini_consumer.rs) | [29](implementation/29-gemini.md) | [29](implementation/29-gemini.md) |
| [tests/support/hooks_consumer.rs](../reference/tests/support/hooks_consumer.rs) | [18](implementation/18-hooks.md) | [47](implementation/47-interruptions.md) |
| [tests/support/input_binding_consumer.rs](../reference/tests/support/input_binding_consumer.rs) | [11](implementation/11-binding.md) | [48](implementation/48-controls.md) |
| [tests/support/lifecycle_consumer.rs](../reference/tests/support/lifecycle_consumer.rs) | [35](implementation/35-integration.md) | [47](implementation/47-interruptions.md) |
| [tests/support/mcp_consumer.rs](../reference/tests/support/mcp_consumer.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [tests/support/mcp_fixture.rs](../reference/tests/support/mcp_fixture.rs) | [32](implementation/32-mcp.md) | [32](implementation/32-mcp.md) |
| [tests/support/model_adapters_consumer.rs](../reference/tests/support/model_adapters_consumer.rs) | [31](implementation/31-xai.md) | [31](implementation/31-xai.md) |
| [tests/support/model_consumer.rs](../reference/tests/support/model_consumer.rs) | [8](implementation/08-model.md) | [12](implementation/12-catalog.md) |
| [tests/support/model_http.rs](../reference/tests/support/model_http.rs) | [26](implementation/26-azure.md) | [54](implementation/54-gemini-schema.md) |
| [tests/support/openai_consumer.rs](../reference/tests/support/openai_consumer.rs) | [25](implementation/25-openai.md) | [25](implementation/25-openai.md) |
| [tests/support/policy_consumer.rs](../reference/tests/support/policy_consumer.rs) | [5](implementation/05-policy.md) | [5](implementation/05-policy.md) |
| [tests/support/recovery_consumer.rs](../reference/tests/support/recovery_consumer.rs) | [24](implementation/24-recovery.md) | [59](implementation/59-host-migration.md) |
| [tests/support/report_process_consumer.rs](../reference/tests/support/report_process_consumer.rs) | [35](implementation/35-integration.md) | [47](implementation/47-interruptions.md) |
| [tests/support/resume_consumer.rs](../reference/tests/support/resume_consumer.rs) | [17](implementation/17-resume.md) | [48](implementation/48-controls.md) |
| [tests/support/routing_consumer.rs](../reference/tests/support/routing_consumer.rs) | [14](implementation/14-routing.md) | [48](implementation/48-controls.md) |
| [tests/support/skills_consumer.rs](../reference/tests/support/skills_consumer.rs) | [21](implementation/21-skills.md) | [47](implementation/47-interruptions.md) |
| [tests/support/source_consumer.rs](../reference/tests/support/source_consumer.rs) | [20](implementation/20-sources.md) | [47](implementation/47-interruptions.md) |
| [tests/support/sqlite_consumer.rs](../reference/tests/support/sqlite_consumer.rs) | [13](implementation/13-sqlite.md) | [48](implementation/48-controls.md) |
| [tests/support/state_consumer.rs](../reference/tests/support/state_consumer.rs) | [6](implementation/06-state.md) | [48](implementation/48-controls.md) |
| [tests/support/tool_loop_consumer.rs](../reference/tests/support/tool_loop_consumer.rs) | [16](implementation/16-tools.md) | [47](implementation/47-interruptions.md) |
| [tests/support/tool_schema_consumer.rs](../reference/tests/support/tool_schema_consumer.rs) | [9](implementation/09-schema.md) | [59](implementation/59-host-migration.md) |
| [tests/support/verification_consumer.rs](../reference/tests/support/verification_consumer.rs) | [23](implementation/23-verification.md) | [47](implementation/47-interruptions.md) |
| [tests/support/version_matrix_consumer.rs](../reference/tests/support/version_matrix_consumer.rs) | [34](implementation/34-evidence.md) | [42](implementation/42-options.md) |
| [tests/support/vertex_consumer.rs](../reference/tests/support/vertex_consumer.rs) | [30](implementation/30-vertex.md) | [30](implementation/30-vertex.md) |
| [tests/support/xai_consumer.rs](../reference/tests/support/xai_consumer.rs) | [31](implementation/31-xai.md) | [31](implementation/31-xai.md) |

## API reference

완성 workspace에서 `cargo doc --workspace --no-deps --locked`를 실행해 public signature와 field 문서를 생성한다. `pub use`로 공개된 경로를 사용하며 내부 module 배치를 extension API로 삼지 않는다.

## 구현 책임별 읽기

1. serialization·canonical·execution_contracts: 원문, 정규화, identity와 segment 자료형.
2. state/execution·SQLite: transaction, command, ownership, legacy 읽기와 이관.
3. model_options·provider_tool_schema: 옵션 provenance와 가역 codec.
4. model_execution/prepared·ToolSet: 고정 입력, physical attempt와 도구 실행의 연결.
5. context_fragment·context_source: revision·tombstone·lineage와 권한.
6. agent/interruption·control·resume: 현재 주체, 원 execution 주체, segment 결과.
7. inspection·agent/inspection: 저장 근거의 순수 변환과 공개 범위.
8. provider codec·tests: 실제 wire 표현과 오류 경계.
9. tests/support와 package script: public API를 사용하는 외부 Host.

- [docs/adapters.md](../reference/docs/adapters.md)
- [docs/agents.md](../reference/docs/agents.md)
- [docs/anthropic.md](../reference/docs/anthropic.md)
- [docs/artifacts.md](../reference/docs/artifacts.md)
- [docs/azure-openai.md](../reference/docs/azure-openai.md)
- [docs/bedrock.md](../reference/docs/bedrock.md)
- [docs/compatibility.md](../reference/docs/compatibility.md)
- [docs/context-compaction.md](../reference/docs/context-compaction.md)
- [docs/context-sources.md](../reference/docs/context-sources.md)
- [docs/context.md](../reference/docs/context.md)
- [docs/contracts.md](../reference/docs/contracts.md)
- [docs/env/README.md](../reference/docs/env/README.md)
- [docs/env/anthropic.md](../reference/docs/env/anthropic.md)
- [docs/env/azure-openai.md](../reference/docs/env/azure-openai.md)
- [docs/env/bedrock.md](../reference/docs/env/bedrock.md)
- [docs/env/gemini.md](../reference/docs/env/gemini.md)
- [docs/env/openai.md](../reference/docs/env/openai.md)
- [docs/env/vertex-ai.md](../reference/docs/env/vertex-ai.md)
- [docs/env/xai.md](../reference/docs/env/xai.md)
- [docs/event-consumers.md](../reference/docs/event-consumers.md)
- [docs/execution-records.md](../reference/docs/execution-records.md)
- [docs/gemini.md](../reference/docs/gemini.md)
- [docs/hooks.md](../reference/docs/hooks.md)
- [docs/input-binding.md](../reference/docs/input-binding.md)
- [docs/installation.md](../reference/docs/installation.md)
- [docs/integration-validation.md](../reference/docs/integration-validation.md)
- [docs/interruption-policy.md](../reference/docs/interruption-policy.md)
- [docs/mcp.md](../reference/docs/mcp.md)
- [docs/migration-v0.2.md](../reference/docs/migration-v0.2.md)
- [docs/model-catalog.md](../reference/docs/model-catalog.md)
- [docs/model-providers.md](../reference/docs/model-providers.md)
- [docs/model-routing.md](../reference/docs/model-routing.md)
- [docs/model-support.md](../reference/docs/model-support.md)
- [docs/openai.md](../reference/docs/openai.md)
- [docs/policy.md](../reference/docs/policy.md)
- [docs/prepared-model-steps.md](../reference/docs/prepared-model-steps.md)
- [docs/provider-setup.md](../reference/docs/provider-setup.md)
- [docs/provider-tool-schemas.md](../reference/docs/provider-tool-schemas.md)
- [docs/quickstart.md](../reference/docs/quickstart.md)
- [docs/recovery.md](../reference/docs/recovery.md)
- [docs/releases.md](../reference/docs/releases.md)
- [docs/run-controls.md](../reference/docs/run-controls.md)
- [docs/skills.md](../reference/docs/skills.md)
- [docs/sqlite-state-store.md](../reference/docs/sqlite-state-store.md)
- [docs/state.md](../reference/docs/state.md)
- [docs/step-inspection.md](../reference/docs/step-inspection.md)
- [docs/tool-inputs.md](../reference/docs/tool-inputs.md)
- [docs/verification.md](../reference/docs/verification.md)
- [docs/vertex.md](../reference/docs/vertex.md)
- [docs/xai.md](../reference/docs/xai.md)
