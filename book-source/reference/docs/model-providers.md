# Model provider adapters

Wickle's core consumes `ModelPort` events and owns the agent loop, Tool execution, budgets and retry policy. Provider crates own credentials, endpoint contracts and wire encoding. Install the adapters your Host needs; provider SDK/HTTP dependencies stay outside the core crate.

| Provider | Crate and guide | Implemented inference contract |
| --- | --- | --- |
| OpenAI | [wickle-model-openai](openai.md) | Responses |
| Azure OpenAI | [wickle-model-azure-openai](azure-openai.md) | Azure Responses v1, explicit deployment |
| Anthropic | [wickle-model-anthropic](anthropic.md) | Messages, API 2023-06-01 |
| AWS Bedrock Claude | [wickle-model-bedrock](bedrock.md) | Runtime/Mantle Messages or Runtime InvokeStream |
| Gemini API | [wickle-model-gemini](gemini.md) | v1/v1beta streamGenerateContent |
| Vertex AI Gemini | [wickle-model-vertex](vertex.md) | v1 streamGenerateContent |
| xAI Grok | [wickle-model-xai](xai.md) | Responses v1 |

## Connect an adapter

1. Construct its connection with explicit Host credentials, scope and target settings. Constructors do not invoke models or load `.env`.
2. Register `connection.binding()`, `connection.target()` and the adapter's API contract in the [model catalog](model-catalog.md). Keep model release, deployment/profile selector, API version and credential revision separate.
3. Create the corresponding model adapter and register it as a scoped `ModelDispatcherEntry` in `RegistryModelDispatcher`. A provider, adapter version, connection revision or scope mismatch cannot select a different account automatically.
4. Register the appropriate metadata inspector and configure the [router's version/capability policy](model-routing.md). Metadata availability alone is not proof of an immutable release or inference permission.
5. Supply that router/dispatcher through the [Agent runtime configuration](agents.md). The core validates complete model output before creating Tool execution plans.

The Host controls configuration and credential refresh. [Environment examples](provider-setup.md) belong to the live-test Host only; production applications can use their own secret/configuration systems.

## Contracts are provider-specific

Shared encoding does not imply identical service features. OpenAI/Azure use the original Responses mode; xAI adds separate reasoning identity and usage handling. Anthropic/Bedrock share Messages content while authentication, framing and metadata differ. Gemini/Vertex share content and SSE handling while their function schemas and output formats differ.

For example, direct Gemini API v1 cannot express object closure in OpenAPI parameters. Its compiler preserves that rule in canonical context and core validation; v1beta can also express it in its native JSON Schema declaration. Vertex v1 supports JSON Schema function declarations. Mantle Messages rejects native JSON-schema output. Bedrock inference profile identity is distinct from the underlying model release and request origin region.

All adapters enforce scope/target/API matching, finite deadlines, bounded streams and no hidden transport retries. Provider-native tools, implicit server conversation state and automatic truncation are not substitutes for the core loop. Consult each guide for supported options and explicit limitations.

## Verification boundaries

The independent package check runs each provider's HTTP/SSE consumer and a common dispatcher conformance consumer across all seven concrete adapters. The latter checks binding isolation and rejects foreign scopes, unknown credential revisions and incompatible API contracts before network access. Provider fixtures separately exercise successful encoding, streaming, Tool replay, usage and failures.

These checks establish library and transport-contract behavior. They do not establish every model version's live availability, account permissions or production support. Actual model/account verification and support promotion require separate evidence; missing metadata remains unknown.

See the [model-version evidence matrix](model-support.md) for selected release identities and the distinction between local contract checks and live smoke results.

Tool schema compilation and saved argument decoding are described in
[provider Tool contracts](provider-tool-schemas.md). Unsupported schema keywords
are retained as canonical constraints rather than causing a Tool to disappear.
