# Vertex AI Gemini adapter

`wickle-model-vertex` implements `ModelPort` for Vertex AI's `v1` `streamGenerateContent`. The Host supplies Google Cloud OAuth credentials, project and optional location/endpoint settings. The library does not load ADC, execute `gcloud`, or read environment files.

```rust,ignore
use std::sync::Arc;
use wickle_model_vertex::{VertexConnection, VertexModel, VertexOptions};

let connection = VertexConnection::new(
    scope, connection_ref, token_provider,
    VertexOptions::new(project_id), // location defaults to global
)?;
let model = Arc::new(VertexModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()` and `connection.api_contract()` with the catalog. The target includes project, location, inference origin, metadata origin and optional quota project. A changed target or credential revision needs a matching route. Only `v1` is implemented; unsupported API versions fail at construction.

## Credentials and endpoints

Implement `VertexTokenProvider` using the Host's ADC or OAuth library. It receives the connection scope, project, location, inference/metadata audience, cancellation and deadline. Return a `VertexToken` with the actual expiry when known. The adapter checks expiry after token resolution, sends Bearer authentication and makes no hidden refresh retry. Static tokens also implement the provider interface. Token Debug output is redacted.

An optional `quota_project` becomes `x-goog-user-project`. The resource project remains part of the request path. Token resolution and HTTP both obey the call deadline; redirects and HTTP retries are disabled.

| Location | Default inference origin |
| --- | --- |
| `global` | `https://aiplatform.googleapis.com/` |
| A single region, such as `us-central1` | `https://us-central1-aiplatform.googleapis.com/` |
| `us` | `https://aiplatform.us.rep.googleapis.com/` |
| `eu` | `https://aiplatform.eu.rep.googleapis.com/` |

The path is `/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:streamGenerateContent?alt=sse`. A bare model ID or one `models/` prefix is accepted. Custom inference and metadata origins are explicit options. Model metadata defaults to the global control plane. These settings select request targets; account permission and model support at a location require separate evidence.

The test Host uses `global` by default, so no location environment variable is required. Applications can still select another supported location through connection options. See the [test environment guide](env/vertex-ai.md).

## Vertex wire differences

The adapter reuses [Gemini content and SSE handling](gemini.md), with explicit Vertex differences:

- Vertex `v1` supports `parametersJsonSchema`, including closed Tool input objects. The direct Gemini API `v1` OpenAPI limitation does not apply.
- Native JSON output uses `generationConfig.responseFormat: [{"text":{"mimeType":"APPLICATION_JSON","schema":...}}]`. Deprecated responseMimeType/responseJsonSchema fields are not sent alongside it.
- Function argument streaming is disabled explicitly. Complete calls may retain `willContinue: false` and empty `partialArgs` in signed replay. An unfinished call or nonempty partialArgs is rejected instead of becoming executable arguments.
- Signed parts and locally assigned IDs retain the same protected replay behavior. IDs absent from the provider response are not injected into the wire, and parallel Tool results are ordered by their original calls.

The supported schema subset, logical thinking/sampling options, single-candidate requirement and transport bounds are described in the Gemini guide. The catalog must restrict capabilities and option values for the selected Vertex model release. Service-native tools and multimodal/asynchronous function responses are not enabled.

## Model inspection

`VertexInspector` requests `publishers/google/models/{model}` with `PUBLISHER_MODEL_VERSION_VIEW_BASIC`. `VertexSnapshot` supplies explicit evidence linking the observed publisher artifact versionId to an immutable inference modelVersion and supported locations. Those version identifiers are separate namespaces; neither a stable versionState nor metadata availability is enough to equate them.

Unknown location/release evidence stays `Unknown`/`Unverified`. Known unsupported locations and missing models are unavailable; changed publisher artifact IDs are version drift. Register the inspector with the router to enforce those observations. A successful metadata request still does not establish inference permission, quota or live availability for an account.

Local HTTP fixtures and an extracted-package consumer verify transport, OAuth expiry/cancellation, target binding, signed calls, output encoding and metadata behavior. Live account/model verification remains separate.

Official contracts: [endpoints and locations](https://docs.cloud.google.com/gemini-enterprise-agent-platform/resources/locations), [v1 discovery](https://aiplatform.googleapis.com/$discovery/rest?version=v1), [publisher metadata](https://docs.cloud.google.com/gemini-enterprise-agent-platform/reference/rest/v1/publishers.models/get), [ADC](https://docs.cloud.google.com/docs/authentication/application-default-credentials).
