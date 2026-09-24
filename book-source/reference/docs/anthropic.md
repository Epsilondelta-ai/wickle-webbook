# Anthropic Messages adapter

`wickle-model-anthropic` implements `ModelPort` for `/v1/messages` with the
`2023-06-01` API contract. The application supplies scoped credentials and connection
settings. The crate does not read environment files or run a separate service.

```rust,ignore
use std::sync::Arc;
use wickle_model_anthropic::{AnthropicConnection, AnthropicModel, AnthropicOptions};

let connection = AnthropicConnection::new(
    scope.clone(), connection_ref, &api_key,
    AnthropicOptions { workspace_id, ..Default::default() },
)?;
let model = Arc::new(AnthropicModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()` and
`AnthropicConnection::api_contract()` in the catalog. The connection uses Bearer
authentication. Set `workspace_id` for a multi-workspace key; it participates in the
target identity. Scope, route and input validation precede HTTP. A call makes one
POST, with redirects and transport retries disabled. [Authentication](https://platform.claude.com/docs/en/manage-claude/authentication).

## Messages and options

Initial system messages become the top-level `system` field. User content is text
or serialized JSON. Client tools use `input_schema`; their results become user
`tool_result` blocks. The default native Tool compiler retains the complete model-owned JSON Schema,
including optional parameters, under non-strict tool use. System-bound fields stay
outside it. The core validates the original contract before binding system inputs
and executing a Tool; provider schema acceptance is not execution authorization.
[Tool definitions](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools).
The adapter does not enable server tools, server-side fallback, or assistant prefill.

Supported logical options are `effort`, `thinking_mode`, and
`thinking_budget_tokens`. Effort maps to `output_config.effort`; modes map to
`thinking.type`, and a manual budget maps to `thinking.budget_tokens`. Model and
binding schemas must restrict options for the selected release. `adaptive` is a
thinking mode, not an effort. Opus 5 runs with thinking by default; omitting the
thinking option preserves that behavior. Unsupported combinations, including manual
thinking on Opus 5 and disabled thinking with its highest effort levels, are rejected.
Opus 5.5 accepts adaptive thinking or omission of the thinking option; disabled
thinking and manual budgets are rejected at every effort level. The adapter does
not replace an omitted effort with a model-independent default.
[Opus 5.5 changes](https://platform.claude.com/docs/en/models/opus-5-5/whats-new-opus-5-5),
[Effort](https://platform.claude.com/docs/en/build-with-claude/effort),
[Opus 5 behavior](https://platform.claude.com/docs/en/models/opus-5/whats-new-opus-5).

`ModelOutput::JsonSchema` maps to `output_config.format` with `type: json_schema`.
Objects must be closed. Constraints outside the supported subset are rejected
instead of being silently removed or changed into prompt text. Core output
verification remains separate from a provider's JSON-format guarantee.
[Structured outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs).

## Streaming and replay

The decoder follows message and content-block lifecycles. Thinking blocks can
precede visible text and can have empty thinking text with a nonempty signature.
Thinking and redacted-thinking data are stored in a route-bound opaque continuation,
then replayed in order with tool results. They are not emitted as visible answer
text. Normalized text and Tool arguments must agree with that saved continuation.
Direct tool-caller metadata is retained; server callers and toolset members are not
converted into local tool requests. [Streaming](https://platform.claude.com/docs/en/build-with-claude/streaming),
[Tool-use response type](https://github.com/anthropics/anthropic-sdk-python/blob/main/src/anthropic/types/tool_use_block.py).

Completion is emitted only after a valid terminal and clean transport EOF.
Duplicate terminals, unsigned completed thinking, open or misordered blocks,
unknown native tools, and exceeded limits cannot become completed Tool plans.
A length-limited or transport-truncated Tool input remains incomplete.
Cancellation and deadlines drop the HTTP response. Provider error messages are not
exposed as model text. Length limits, refusals and unsupported continuation modes
remain distinct from successful completion.

A complete Tool proposal with malformed JSON, duplicate keys, a non-object value,
or precision-losing numbers reaches the bounded core repair loop without execution.
Both accumulated deltas and populated initial inputs retain their raw argument
text. The surrounding SSE envelope still undergoes strict validation.

Normal stored continuations keep their existing format. Invalid arguments use a
versioned protected replay record that checks call identity and empty normalized
placeholders before encoding an `INVALID_JSON` wrapper. The matching `tool_result`
sets `is_error: true` and includes the original argument text with the core feedback.
Thinking signatures and unrelated valid calls are preserved. This supports invalid
input recovery without enabling eager streaming or running invalid Tool handlers.
[Invalid Tool input handling](https://platform.claude.com/docs/en/agents-and-tools/tool-use/fine-grained-tool-streaming#handling-invalid-json-in-tool-responses).

Usage updates are cumulative, not per-event increments. The adapter records the
reported input/output counts and actual model ID. It does not fill in a separate
unreported model version. Additional cache accounting fields are not normalized into
the core's two token-count fields.

## Model inspection and live verification

`AnthropicInspector` combines `/v1/models/{id}` availability with explicit
`AnthropicSnapshot` evidence supplied by the Host. Unknown versions remain unknown.
A dateless ID is not automatically an alias: Anthropic documents canonical IDs from
the 4.6 generation onward as pinned snapshots. Register releases from trusted
metadata rather than guessing from date patterns. [Model ID rules](https://platform.claude.com/docs/en/about-claude/models/model-ids-and-versions).

Contract fixtures and the extracted-package consumer validate the local integration.
Actual model/account availability is a separate live check. The [environment guide](env/anthropic.md)
describes test-Host inputs; an environment file is not a runtime dependency of the
library.
