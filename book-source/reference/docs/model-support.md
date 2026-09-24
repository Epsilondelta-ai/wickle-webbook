# Model versions and validation evidence

Wickle does not install a default model catalog or silently choose the newest release. The Host registers model identities, binding revisions, API contracts, target restrictions and capability schemas. Register another release as another definition/binding; keep the saved catalog and route for an existing Run. See [model routing](model-routing.md) and [provider adapters](model-providers.md).

## Validation levels

| Level | Meaning |
| --- | --- |
| `planned` | Declared configuration without sufficient passing contract evidence |
| `contract_tested` | The stated local contract checks passed; account access and remote model availability remain unverified |
| `live_verified` | The stated real-service checks passed for that configuration; the label does not transfer to other versions, regions, credentials or untested features |

`ModelBinding` evidence is tied to its contract digest. `min_support` can reject planned or contract-only bindings. A local fixture cannot establish real model availability, an immutable provider version or production readiness.

## Selected release matrix

The following identities are exercised by the provider tests. Every row has local HTTP/SSE request/response identity checks for both listed releases. These checks use bounded local servers, not vendor accounts. Broader tool, replay, metadata and failure contracts are covered by each adapter's existing tests, with their own scope.

| Provider path | Model IDs | API contract exercised by the two-release check | Target scope of fixtures | Evidence level |
| --- | --- | --- | --- | --- |
| OpenAI | `gpt-6-astra`, `gpt-5.6-sol` | Responses `v1` | Public-API shape; no regional availability claim | Astra: observed smoke below; Sol: local only |
| Azure OpenAI | `gpt-6-astra`, `gpt-5.6-sol` | Responses `v1` | Two deployment selectors with fixture revisions `2026-09-03` and `2026-07-09` | Astra: observed deployment smoke; Sol: local only |
| Anthropic | `claude-opus-5`, `claude-opus-4-8` | Messages `2023-06-01` | Direct-API shape; no regional availability claim | Opus 5: observed smoke; Opus 4.8: local only |
| AWS Bedrock | `anthropic.claude-opus-5`, `anthropic.claude-opus-4-8` | Native Messages `2023-06-01` | Runtime and Mantle; fixture origin region `us-east-1`, not verified inference residency or access | `contract_tested` |
| Gemini API | `gemini-3.8-flash`, `gemini-3.7-flash` | `stream_generate_content` / `v1beta` | Two-release fixture uses v1beta; 3.8 additionally exercised through v1 | 3.8: observed v1/v1beta smokes; 3.7: local only |
| Vertex AI | `gemini-3.8-flash`, `gemini-3.7-flash` | `stream_generate_content` / `v1` | `global` with a fixture project; account availability unverified | `contract_tested` |
| xAI | `grok-4.6`, `grok-4.5` | Responses `v1` | Public-API shape; cluster availability is configuration-specific | 4.6: observed smoke; 4.5: local only |

Model identifiers and platform distinctions were checked against the official [OpenAI Astra](https://developers.openai.com/api/docs/models/gpt-6-astra) and [Sol](https://developers.openai.com/api/docs/models/gpt-5.6-sol) pages, [Azure model catalog](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/concepts/models-sold-directly-by-azure), [Claude migration guide](https://platform.claude.com/docs/en/models/opus-5/migration-guide), [Claude on Bedrock](https://platform.claude.com/docs/en/build-with-claude/claude-in-amazon-bedrock), [Gemini catalog](https://ai.google.dev/gemini-api/docs/models), [Vertex 3.7](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-7-flash) and [3.8](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-8-flash) pages, and [xAI 4.5](https://docs.x.ai/developers/models/grok-4.5) and [current models](https://docs.x.ai/developers/models). Vendor documentation is not a Wickle live-test result.

## Observed live smoke checks

The [sanitized observation summary](validation/provider-smoke-summary.json)
records synthetic read-only Tool loops from pre-merge development executions.
Each successful case made two model requests, two metadata requests and one Tool
execution, produced the expected calculated result, reopened SQLite successfully,
and made no extra calls when the same Start request was repeated.

| Path | Model and options | Observed API |
| --- | --- | --- |
| OpenAI | `gpt-6-astra`, medium reasoning effort, 4,096 output-token limit | Responses v1 |
| Azure OpenAI | `gpt-6-astra`, medium reasoning effort, 4,096 output-token limit | Responses v1 on one explicit resource deployment |
| Anthropic | `claude-opus-5`, medium effort, 4,096 output-token limit | Messages 2023-06-01 |
| Gemini API | `models/gemini-3.8-flash`, medium thinking, 4,096 output-token limit | Separate v1 and v1beta checks |
| xAI | `grok-4.6`, high reasoning effort, 4,096 output-token limit | Responses v1 |

The summary preserves Run start timestamps and actual reported model/version
fields. Its `merged_implementation_commit` locates the resulting change; it is
not a claim that the live run used that exact post-merge checkout. It omits credentials, account/deployment targets,
request identifiers, database paths and generated content. These observations do
not automatically establish `live_verified` for a newly versioned binding or a
different account. They make no general immutable-release or production-readiness
claim. Full JSON-output and failure behavior are separate contracts; a successful
Tool loop does not prove every feature combination.

Bedrock had no configured model selector for the new contract check. During these checks,
Vertex inference was not attempted because credential refresh/project access was
unavailable. Their local contract results remain valid and their new live checks
remain unverified. Earlier version results do not replace these checks.

The earlier [OpenAI native JSON smoke](validation/openai-live-smoke.json) remains
available as a distinct historical observation. Missing reported versions remain
unknown rather than being filled from requested selectors.

## Coexistence, upgrades and limits

The [version-matrix consumer](../tests/support/version_matrix_consumer.rs) registers 14 identities in one catalog across seven provider namespaces. Its release revisions, capability differences, target and evidence records are synthetic test data. They exercise coexistence, provider-scoped aliases, changed capability rejection, exact catalog restoration, target tampering, lifecycle rejection and support-level gates; they are not vendor capability declarations.

Provider tests separately pass both documented model IDs through their concrete adapters. Azure keeps the deployment selector separate from the reported model; Gemini keeps the requested model separate from the reported revision. Missing model versions are not filled from requested values.

Gemini 3.7 and 3.8 both reject `minimal` thinking before dispatch. This includes [Gemini 3.7's documented restriction](https://ai.google.dev/gemini-api/docs/models/gemini-3.7-flash), rather than applying only the newest model's rule. For other per-release options, register accurate capability schemas and retain provider-specific adapter validation. Do not copy one release's option schema to another without verification.

Run the adapter tests with `cargo test --workspace --locked`. Run `python3 scripts/check-package.py --allow-dirty` for the extracted-package consumers. See [test environment setup](provider-setup.md) to conduct additional live checks in a separate Host; the library does not read `.env`.
