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

The core obtains a versioned `GeminiToolSchemaCompiler` from the model port. It preserves supported input constraints and supplies canonical text constraints for validation that cannot be expressed natively. Only model-owned parameters enter this contract; system inputs are bound by the core after validation.

| Declaration | `v1` | `v1beta` |
| --- | --- | --- |
| Function input | `parameters` using OpenAPI Schema | `parametersJsonSchema` |
| Closed objects | Enforced by the core; described in additional constraints | Also represented natively |
| Numeric enum | Described in additional constraints and validated by the core | Also represented natively |
| Unrepresentable object or tuple field | Reversible JSON text field | Reversible JSON text field when needed |
| No exposed parameters | No parameter declaration | No parameter declaration |

Optional fields remain optional and explicit null remains distinct from absence. Supported bounds, patterns and finite local references are preserved; reference expansion has depth, node and byte limits. Unsupported constraints remain in the original contract and its additional text. A complete but invalid call produces repair feedback without executing the Tool. No automatic API-version switch occurs.

Direct protocol consumers must supply compiled tools and their additional context fragments, decode arguments using the saved contract, and validate against the original Tool schema. The encoder alone does not perform this core pipeline. `protocol::encode_request` and `encode_vertex_request` return serialized JSON bytes so invalid numeric tokens in signed replay data remain exact; send these bytes as the HTTP body without serializing them again.

`ModelOutput::JsonSchema` requests native structured output. Model-specific combinations of tools and structured output must be enabled through the catalog only when supported by that model. The core still validates arguments before Tool execution and verifies final output separately.

## Thinking, tools and replay

Logical `thinking_level` maps to `thinkingConfig.thinkingLevel`; `thinking_budget_tokens` maps to `thinkingBudget`. Choose one. `temperature` and `top_p` map to the corresponding generation settings. Unknown options fail before HTTP. The catalog must further restrict values to the selected model release; for example, the adapter rejects `minimal` for `gemini-3.8-flash`.

System messages become `systemInstruction`. Tool declarations contain only the projected model input schema. Standard function calls become proposals for the core's Tool boundary; native code execution, search and other service tools are not enabled.

Signed parts, including thought signatures attached to text or function calls, are retained in an opaque continuation bound to the exact route. Thought text is excluded from visible answer deltas. The adapter verifies visible text and calls against the stored parts before replay. It neither merges signed parts nor invents signatures. Complete calls with invalid argument shapes, duplicate keys or nonrepresentable numeric tokens retain their original signed part as protected raw JSON. Repair requests replay that part exactly, including after protected serialization; malformed outer envelopes still fail as protocol errors.

When a response omits a function call ID, the adapter assigns a local projection ID. That ID pairs core calls with results and is not injected into the provider's original signed parts or functionResponse. Parallel results are emitted in original call order, including results delivered as separate Tool messages, so same-name calls without wire IDs remain associated correctly.

## Completion, usage and metadata

One invocation makes one HTTP request, with redirects and transport retries disabled. Unclassified HTTP 400 errors remain Unsupported; the adapter does not infer context overflow by matching provider error text. Cancellation, deadlines, SSE/frame-count limits and byte limits bound it. A recognized finish reason and clean EOF are both required. Truncated, refused, malformed or ambiguous multi-candidate responses cannot supply an executable successful continuation.

`responseId` and `modelVersion` populate the corresponding reported metadata; missing values are not filled from the request. Output usage includes thinking tokens when the reported counts establish the total. Partial or absent counts remain unknown. Repeated cumulative usage is not summed across frames.

`GeminiInspector` reads the selected API's Models resource and checks the model name, generation method and registered metadata version. `GeminiSnapshot` links the metadata version to an explicitly documented immutable inference release. The Models API `version` and inference `modelVersion` are separate fields; availability or a version-looking name alone is not immutable-release evidence. Unregistered releases remain unverified.

Local HTTP fixtures and an extracted-package consumer verify these contracts. Account/model availability and live inference require separate verification. See the [test environment guide](env/gemini.md).

Official contracts: [generateContent](https://ai.google.dev/api/generate-content), [v1 discovery](https://generativelanguage.googleapis.com/$discovery/rest?version=v1), [v1beta discovery](https://generativelanguage.googleapis.com/$discovery/rest?version=v1beta), [Models API](https://ai.google.dev/api/models), [thinking](https://ai.google.dev/gemini-api/docs/thinking).
