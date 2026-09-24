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
conversation and context limits. The adapter provides a versioned Tool schema
compiler; normal Agent calls use its frozen projection with `strict: true`.
The wire encoder checks compatibility without rewriting that projection. Direct
`ModelPort` callers that supply uncompiled schemas get explicit `strict: false`
when their schema does not meet the strict subset. Structured output uses `strict: true` and rejects unsupported structural forms rather than
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

## Tool inputs and repair

Define Tools with their ordinary canonical JSON Schema and model-visible
parameters. The core compiles that projection for the selected provider, model
release, and capability revision. System-bound fields and values stay outside it.

The OpenAI compiler preserves supported native constraints. For optional values,
a `{ "present": false, "value": null }` envelope means omission; `present: true`
with a null value means explicit null. Nested optional objects, open objects, and
forms that cannot retain their shape under strict mode use a JSON string. A
required JSON-text field contains the value; an optional one contains `[]` for
omission or `[value]` for presence. Application Tool executors always receive the
restored ordinary values, with defaults and system inputs bound by the core.

Unsupported validation keywords remain in the original contract and in the
model's constraint context. They are never discarded as execution requirements.
The compiler uses the documented smaller native subset for fine-tuned models;
large enums and deeply nested shapes can use JSON text to fit provider limits.
Malformed arguments, precision-losing numbers, and original-contract violations
reach the bounded core repair loop without executing the Tool. Complete malformed
arguments remain distinct from truncated or conflicting SSE envelopes, which are
protocol failures. Opaque replay preserves the original argument bytes, including
invalid proposals, so the model can receive their repair feedback.

[Strict Tool rules](https://developers.openai.com/api/docs/guides/function-calling),
[Supported schema subset and limits](https://developers.openai.com/api/docs/guides/structured-outputs).

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
