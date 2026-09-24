# Wickle

**English** | [한국어](README.ko.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

**An extensible Rust agent engine.**

Embed agents in your application with configurable profiles, model providers and tools. Wickle runs the model/tool loop inside your process; your application supplies data access, credentials and authorization.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## Installation

Requires Rust 1.85 or later and a Tokio runtime. Use the same Git tag for the core and optional adapters. v0.2.0 is distributed through GitHub Releases.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
```

## Run an agent

Create an `AgentProfile` for instructions, selected tools, model binding and execution limits. Configure `AgentBindings` with your model, state store, policy and other Host components, then submit a `RunRequest` with an authenticated `ExecutionContext`.

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

`start` returns a handle to the Run; `outcome` returns its saved result. `Guarded::ApprovalRequired` means the application must obtain approval. The example accepts components configured by your application; see the agent guide for binding setup.

Run an example without model credentials: `python3 scripts/check-package.py --consumer agent` from a source checkout. See the [quickstart](docs/quickstart.md).

## Features

- **Tool input ownership:** expose only model-owned arguments; inject workspace IDs, user IDs and other trusted values through registered system inputs.
- **Persistent execution:** store outcomes and events, wait for approvals or input, and explicitly resume or recover interrupted Runs.
- **Extensibility:** connect tools, read-only context sources, Skills, lifecycle hooks and MCP stdio tools.
- **Bounded execution:** limit model calls, tool attempts, repairs and elapsed time; use shared budgets for compression and verification.
- **Scoped access:** separate organizations and workspaces through Host policy and scoped bindings.
- **Context and output:** preserve artifacts and evidence, compact context, and validate structured outputs or verifier criteria.

- **Tool schema adaptation:** preserve canonical constraints while compiling schemas and argument encodings for each provider.
- **Explicit model options:** override binding defaults with Profile and Run settings, with stored option provenance.
- **Interruption and inspection:** attach application state to recoverable stops and inspect saved steps with sensitive fields redacted.

## Model providers and adapters

Optional crates connect the core to the following services. Model versions, deployment names, API contracts and provider options remain explicit. Consult the provider guide for supported operations and model-specific limitations.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

Use `wickle-state-sqlite` for local persistence and `wickle-adapter-runtime` to assemble scoped extensions. Memory and graph services can implement `ContextSource` or tools; post-run writes belong to an external event consumer.

## Documentation

- [Quickstart](docs/quickstart.md)
- [Migrate to v0.2.0](docs/migration-v0.2.md)
- [Provider Tool contracts](docs/provider-tool-schemas.md)
- [Interruption policies](docs/interruption-policy.md)
- [Saved-step inspection](docs/step-inspection.md)

- [Install and configure dependencies](docs/installation.md)
- [Agent bindings, requests and results](docs/agents.md)
- [Tools and system inputs](docs/tool-inputs.md)
- [Context sources and memory](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [Model routing](docs/model-routing.md)
- [Provider configuration and support](docs/model-providers.md)
- [SQLite persistence and recovery](docs/sqlite-state-store.md)
- [MCP tools](docs/mcp.md)
- [Artifacts](docs/artifacts.md)
- [Context compaction](docs/context-compaction.md)
- [Output verification](docs/verification.md)

## License

[MIT](LICENSE) · MIT © EpsilonDelta. The 0.2 API may change in later releases.
