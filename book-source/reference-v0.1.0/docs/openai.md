# OpenAI Responses adapter

`wickle-model-openai` implements `ModelPort` over the Responses API's HTTP/SSE
transport. The HTTP client and credentials remain in this optional crate; the core
has no OpenAI SDK or HTTP dependency.

```rust,ignore
use std::sync::Arc;
use wickle_model_openai::{
    OpenAiConnection, OpenAiInspector, OpenAiModel, OpenAiOptions, OpenAiSnapshot,
};

let connection = OpenAiConnection::new(
    scope.clone(),
    connection_ref,
    &api_key, // Supplied by the application, not loaded by the library.
    OpenAiOptions::default(),
)?;
let model = Arc::new(OpenAiModel::new(connection.clone()));
let inspector = Arc::new(OpenAiInspector::new(connection.clone(), snapshots)?);
let exchange = ModelExchange::new(model, policy)
    .with_route_inspector(inspector, std::time::Duration::from_secs(10))?;
```

Register the exact `connection.binding()`, `connection.target()`, and
`OpenAiConnection::api_contract()` in the model catalog. Organization and project
headers, when configured, are part of the target identity. Each connection belongs
to one explicit Wickle scope and credential revision.

The supported operation is `responses`, API contract `v1`. Chat Completions,
WebSocket, background responses, and provider-native tools are not enabled by this
adapter. [Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create).

## Requests and streaming

The codec supports text/JSON content, function calls and their results, strict
JSON-schema output, and route-bound continuation replay. It maps these Host options
explicitly: `reasoning_effort`, `temperature`, `top_p`, and `verbosity`. Model and
binding option schemas must describe the options supported by each model release.
Unrecognized options are rejected; options cannot replace the input, tools, model,
conversation state, or authentication fields.

Each invocation sends one POST. Redirects and SDK retries are disabled. The request
uses `stream: true`, `store: false`, and `truncation: disabled`; Wickle owns the
conversation and context limits. Function definitions use the original projected
schema with `strict: false`, preserving optional model parameters. Structured
output uses `strict: true` and rejects unsupported structural forms rather than
rewriting the output contract. [Function calling](https://developers.openai.com/api/docs/guides/function-calling),
[structured-output schemas](https://developers.openai.com/api/docs/guides/structured-outputs#supported-schemas).

SSE framing handles split UTF-8, CR/LF boundaries, and multiline data. Function
fragments retain their output index, item ID, and call ID. The complete response
must agree with accumulated content before a completion is emitted. Incomplete,
failed, conflicting, oversized, or truncated streams cannot become executable Tool
plans. Cancellation and deadlines close unfinished streams. Transport byte/frame
limits are separate from the normalized model-event limit.
[Streaming responses](https://developers.openai.com/api/docs/guides/streaming-responses).

Reasoning and assistant output items are preserved in an opaque continuation tied
to the exact route. Replaying it checks the normalized text/calls, then sends the
original items once and in order. This retains encrypted reasoning and message
phase metadata without duplicating the visible assistant content.

## Model versions and inspection

Model identifiers come from the selected route, not a built-in model alias. Versions
can coexist in the catalog. `OpenAiInspector` checks account availability using the
models endpoint. Register `OpenAiSnapshot` entries only for immutable releases
established by provider documentation or another trusted Host evidence source.
The inspector never infers immutability from a date in a name or copies an arbitrary
requested version into its observation.

Without registered snapshot evidence, release information remains unknown and
semantics are `Unverified`. A policy requiring pinned versions will reject that
observation. Provider response metadata records the actual `model` and reported
input/output token counts. Missing usage and separately unreported model versions
remain absent.

For example, the [GPT-6 Astra model page](https://developers.openai.com/api/docs/models/gpt-6-astra)
identifies its current snapshot and supported reasoning efforts. An application can
register that documented snapshot without adding a duplicate VERSION environment
variable. Environment files belong only to the [live test Host](env/openai.md).
