# Wickle

[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | **简体中文** | [Español](README.es.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

**可嵌入 Rust 应用的可扩展智能体引擎。**

组合配置档案、模型和工具，在应用中运行智能体。Wickle 在同一进程内循环执行模型判断和工具调用；数据访问、凭据和授权由应用提供。

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## 安装

需要 Rust 1.85 或更新版本及 Tokio 运行时。核心与适配器使用相同的 Git 标签。v0.2.0 通过 GitHub Releases 发布。

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
```

## 运行智能体

在 `AgentProfile` 中设置指令、工具、模型绑定和执行限制。通过模型、状态存储及授权策略构建 `AgentBindings`，再传入 `RunRequest` 和经过认证的 `ExecutionContext`。

```rust
use wickle::*;

pub async fn run_once(
    profile: AgentProfile,
    bindings: AgentBindings,
    request: RunRequest,
    context: ExecutionContext,
) -> Result<Guarded<RunOutcome>, ContractError> {
    let agent = create_agent(profile, bindings)?;
    match agent.start(request, context.clone()).await? {
        Guarded::Completed(handle) => handle.outcome(&context).await,
        Guarded::ApprovalRequired(challenge) => {
            Ok(Guarded::ApprovalRequired(challenge))
        }
    }
}
```

`start` 返回执行句柄，`outcome` 返回已保存的结果。`Guarded::ApprovalRequired` 表示应用需要取得批准。示例中的组件由应用预先配置，详见运行指南。

无需模型凭据即可运行示例：在源码目录执行 `python3 scripts/check-package.py --consumer agent`。完整步骤见[快速开始](docs/quickstart.md)。

## 主要功能

- **工具输入分离：** 仅向模型公开其负责的参数，工作区 ID 等可信值由系统输入注入。
- **持久执行：** 保存结果与事件，支持批准等待、输入等待、恢复与重新继续。
- **扩展：** 连接工具、ContextSource、Skills、Hooks 和 MCP stdio。
- **执行限制：** 限制模型调用、工具尝试、修复次数与运行时间。
- **访问范围：** 通过 Host 策略隔离组织与工作区。
- **上下文与输出：** 管理原始文件和证据，支持上下文压缩、结构化输出与 verifier。

- **工具模式转换：** 保留原始输入约束，并转换为各提供者支持的模式和参数表示。
- **显式模型选项：** 按绑定默认值、Profile、Run 的顺序覆盖设置，并保存选项来源。
- **中断与诊断：** 为可恢复的中断记录应用状态，查看已保存步骤时隐藏敏感值。

## 模型和适配器

可使用独立 crate 接入以下服务。明确配置模型版本、部署名称、API 契约及提供者选项；具体支持范围与限制见相关指南。

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

使用 `wickle-state-sqlite` 实现本地持久化，使用 `wickle-adapter-runtime` 组装扩展。记忆与图服务通过 ContextSource 或工具连接，执行后的写入由外部事件消费者负责。

## 使用文档

- [快速开始](docs/quickstart.md)
- [迁移到 v0.2.0](docs/migration-v0.2.md)
- [提供者工具契约](docs/provider-tool-schemas.md)
- [中断策略](docs/interruption-policy.md)
- [已保存步骤诊断](docs/step-inspection.md)

- [安装依赖](docs/installation.md)
- [绑定、请求和结果](docs/agents.md)
- [工具与系统输入](docs/tool-inputs.md)
- [上下文和记忆](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [模型路由](docs/model-routing.md)
- [提供者配置与支持范围](docs/model-providers.md)
- [SQLite 与恢复](docs/sqlite-state-store.md)
- [MCP 工具](docs/mcp.md)
- [Artifacts](docs/artifacts.md)
- [上下文压缩](docs/context-compaction.md)
- [输出验证](docs/verification.md)

## 许可证

[MIT](LICENSE) · MIT © EpsilonDelta. 0.2 系列 API 可能在后续版本中调整。
