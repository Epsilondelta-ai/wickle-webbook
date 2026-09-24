# Gemini API adapter

`wickle-model-gemini` implements `ModelPort` for Google's `streamGenerateContent` operation. The Host supplies an API key, scope, connection revision, endpoint and API version. The crate does not read `.env`, select a different model, or run a provider-native agent loop.

```rust,ignore
use std::sync::Arc;
use wickle_model_gemini::{GeminiConnection, GeminiModel, GeminiOptions};

let connection = GeminiConnection::new(
    scope, connection_ref, &api_key,
    GeminiOptions { api_version: "v1beta".into(), ..Default::default() },
)?;
let model = Arc::new(GeminiModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()` and `connection.api_contract()` with the model catalog. A bare model ID or a single `models/` resource prefix is accepted. The adapter normalizes that prefix only when constructing the resource URL. The exact route remains bound to its catalog and API version. Credentials use the `x-goog-api-key` header, never URL query parameters.

## API versions and schemas

The default API version is `v1`; `v1beta` is an explicit Host choice. Both use SSE `streamGenerateContent?alt=sse`. The Interactions API is a different protocol and is not implemented here.

The public `protocol` module also supplies shared wire primitives for the [Vertex AI adapter](vertex.md). Pair `encode_vertex_request` with `GenerateContentDecoder::for_vertex` for Vertex's JSON function schemas, structured output format and complete function-call metadata. The direct Gemini adapter uses `encode_request` and `GenerateContentDecoder::new`; its API-version rules remain as follows.

| Declaration | `v1` | `v1beta` |
| --- | --- | --- |
| Function input | `parameters` using OpenAPI Schema | `parametersJsonSchema` |
| Closed object (`additionalProperties: false`) | Rejected before HTTP | Preserved |
| Numeric enum | Rejected before HTTP | Preserved |
| Structured text output | `responseJsonSchema` and JSON MIME type | Same |

The adapter supports a conservative schema subset: type, properties, required, items, anyOf, enum, description, title, minimum/maximum, minItems/maxItems and format. JSON Schema additionally supports additionalProperties. Unsupported keywords are rejected instead of removed; OpenAPI additionally requires representable types and string enums. Wickle's usual closed Tool input schema therefore requires the JSON Schema declaration path. Configure the catalog's capabilities for the selected API and schema. No automatic version switch occurs.

`ModelOutput::JsonSchema` requests native structured output. Model-specific combinations of tools and structured output must be enabled through the catalog only when supported by that model. The core still validates arguments before Tool execution and verifies final output separately.

## Thinking, tools and replay

Logical `thinking_level` maps to `thinkingConfig.thinkingLevel`; `thinking_budget_tokens` maps to `thinkingBudget`. Choose one. `temperature` and `top_p` map to the corresponding generation settings. Unknown options fail before HTTP. The catalog must further restrict values to the selected model release; for example, the adapter rejects `minimal` for `gemini-3.8-flash`.

System messages become `systemInstruction`. Tool declarations contain only the projected model input schema. Standard function calls become proposals for the core's Tool boundary; native code execution, search and other service tools are not enabled.

Signed parts, including thought signatures attached to text or function calls, are retained in an opaque continuation bound to the exact route. Thought text is excluded from visible answer deltas. The adapter verifies visible text and calls against the stored parts before replay. It neither merges signed parts nor invents signatures.

When a response omits a function call ID, the adapter assigns a local projection ID. That ID pairs core calls with results and is not injected into the provider's original signed parts or functionResponse. Parallel results are emitted in original call order, including results delivered as separate Tool messages, so same-name calls without wire IDs remain associated correctly.

## Completion, usage and metadata

One invocation makes one HTTP request, with redirects and transport retries disabled. Unclassified HTTP 400 errors remain Unsupported; the adapter does not infer context overflow by matching provider error text. Cancellation, deadlines, SSE/frame-count limits and byte limits bound it. A recognized finish reason and clean EOF are both required. Truncated, refused, malformed or ambiguous multi-candidate responses cannot supply an executable successful continuation.

`responseId` and `modelVersion` populate the corresponding reported metadata; missing values are not filled from the request. Output usage includes thinking tokens when the reported counts establish the total. Partial or absent counts remain unknown. Repeated cumulative usage is not summed across frames.

`GeminiInspector` reads the selected API's Models resource and checks the model name, generation method and registered metadata version. `GeminiSnapshot` links the metadata version to an explicitly documented immutable inference release. The Models API `version` and inference `modelVersion` are separate fields; availability or a version-looking name alone is not immutable-release evidence. Unregistered releases remain unverified.

Local HTTP fixtures and an extracted-package consumer verify these contracts. Account/model availability and live inference require separate verification. See the [test environment guide](env/gemini.md).

Official contracts: [generateContent](https://ai.google.dev/api/generate-content), [v1 discovery](https://generativelanguage.googleapis.com/$discovery/rest?version=v1), [v1beta discovery](https://generativelanguage.googleapis.com/$discovery/rest?version=v1beta), [Models API](https://ai.google.dev/api/models), [thinking](https://ai.google.dev/gemini-api/docs/thinking).
