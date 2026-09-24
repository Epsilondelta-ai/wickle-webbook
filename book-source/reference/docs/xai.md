# xAI Grok adapter

`wickle-model-xai` implements `ModelPort` for the xAI `v1` Responses API. The Host supplies the API key, scope, connection revision and endpoint. The library does not load environment files or select a replacement model.

```rust,ignore
use std::sync::Arc;
use wickle_model_xai::{XaiConnection, XaiModel, XaiOptions};

let connection = XaiConnection::new(
    scope, connection_ref, &api_key, XaiOptions::default(),
)?;
let model = Arc::new(XaiModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()` and `connection.api_contract()` in the catalog. The default base URL is `https://api.x.ai/v1/`; only the `v1` contract is implemented. Credentials use Bearer authentication and are not placed in model input or URL queries. Changed endpoints, credentials or scopes require the corresponding binding.

## Stateless Responses and options

Every call sends `store: false`, `stream: true`, `truncation: disabled`, and `include: ["reasoning.encrypted_content"]`. The core owns history and compaction; the adapter does not use previous_response_id, server-native tools or provider-controlled automatic compaction. Redirects and hidden HTTP retries are disabled.

Logical `reasoning_effort` maps to `reasoning.effort`. The service-level options supported here are none/low/medium/high/xhigh; the selected model's catalog must restrict them further. `grok-4.7`, `grok-4.6` and `grok-4.5` reject none because reasoning cannot be disabled. Minimal, max and verbosity are rejected by this adapter. Temperature and top_p are available only when the selected model's catalog permits them. Omitted options remain omitted.

Function declarations contain only model-visible parameters. `XaiToolSchemaCompiler` preserves supported input constraints while keeping the canonical contract in additional context and core validation. Optional fields stay optional, and null stays distinct from omission. Open objects explicitly carry `additionalProperties: true` because the provider has a different default. Single-branch `allOf` constraints remain native. Patterns remain in canonical guidance and core validation because xAI regex matching differs from JSON Schema. Shapes that cannot be represented safely use reversible JSON text fields; local reference expansion is bounded.

The provider makes Tool schema strictness implicit, so the adapter omits the `strict` flag. This does not replace core validation: after decoding the saved contract, invalid arguments receive bounded repair feedback before system inputs are bound or the Tool is executed. Direct `ModelPort` users must perform the same compilation, context inclusion, decoding and original-contract validation steps.

More than 350 declarations fail before HTTP. Native JSON output separately uses the shared Responses JSON-schema subset and strict output format; unsupported output constraints fail explicitly. The core verifies final output independently.

## xAI-specific replay and usage

The shared Responses codec exposes `encode_xai_request` and `ResponsesDecoder::for_xai`. These use a separate replay kind. OpenAI and Azure continue to use their original validation mode.

xAI reasoning items may have a missing or empty ID. The decoder tracks them by output position and preserves the original item for replay, without manufacturing an ID. Executable function-call IDs and message identities retain strict validation. Encrypted reasoning is required for the stateless continuation; missing encrypted state is rejected rather than silently dropping it or exposing reasoning as answer text. Visible text and Tool calls must match the protected original items before replay. Complete invalid argument JSON, including nonrepresentable numeric tokens, is retained as its original string for repair and replay. It does not become executable input.

Modern input_tokens/output_tokens are read directly. For legacy usage, prompt_tokens and total_tokens can establish total output as total minus prompt, including reasoning. Legacy completion_tokens alone does not establish whether all reasoning was counted, so ambiguous output usage remains unknown. Conflicting modern/legacy input counts, inconsistent reported totals and impossible component counts are rejected. Missing values are not filled from reservations or requested limits.

A validated terminal and clean SSE EOF are required. Incomplete, conflicting or malformed protocol envelopes cannot become a successful continuation; complete invalid Tool arguments instead enter the core repair path. Refused output grants no Tool execution. HTTP errors are classified without leaking provider error text; unclassified HTTP 400 responses remain Unsupported.

## Model identity and verification

`XaiInspector` checks Models API availability and requires explicit `XaiSnapshot` evidence to establish an immutable release. A created timestamp, base name, date-looking name or requested version does not create that evidence. Some retired slugs are automatically served by newer models under xAI's published migration policy. The adapter reports the actual response model without replacing the selected route; the core can then detect drift.

Local HTTP/SSE tests and an extracted-package consumer cover the protocol and usage variants. Account/model connectivity and immutable-release promotion require separate live evidence. See the [test environment guide](env/xai.md).

Official contracts: [Responses reference](https://docs.x.ai/developers/rest-api-reference/inference/responses), [text generation and encrypted state](https://docs.x.ai/developers/model-capabilities/text/generate-text), [reasoning](https://docs.x.ai/developers/model-capabilities/text/reasoning), [retired slug migration](https://docs.x.ai/developers/migration/may-15-retirement).

Tool schema contract: [xAI structured outputs and function schema rules](https://docs.x.ai/developers/model-capabilities/text/structured-outputs).
