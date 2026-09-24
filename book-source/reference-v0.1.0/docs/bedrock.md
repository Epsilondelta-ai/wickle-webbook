# AWS Bedrock Claude adapter

`wickle-model-bedrock` provides `ModelPort` and `ModelInspector` implementations. The Host supplies scope, credentials, region, the exact model/profile selector and the selected wire operation. The library reads no environment variables or AWS profiles.

```rust,ignore
use std::sync::Arc;
use wickle_model_bedrock::{
    BedrockConnection, BedrockModel, BedrockOptions, BedrockSelector,
};

let options = BedrockOptions::new(
    "us-east-1", BedrockSelector::InferenceProfile(profile_id),
);
let connection = BedrockConnection::new(scope, connection_ref, credentials, options)?;
let model = Arc::new(BedrockModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()` and `connection.api_contract()` in the model catalog. Keep the underlying model release separate from the exact inference profile ID or ARN. Changing endpoint, operation, region or credential revision requires a matching route.

## Wire operations and authentication

| Endpoint | Operation | Response | SigV4 service |
| --- | --- | --- | --- |
| Runtime | `Messages` (default) | Anthropic SSE | `bedrock` |
| Mantle | `Messages` | Anthropic SSE | `bedrock-mantle` |
| Runtime | `InvokeStream` | AWS event stream | `bedrock` |

Native Messages uses `/anthropic/v1/messages`, header `anthropic-version: 2023-06-01`, and the exact selector in `model`. InvokeStream uses the encoded selector in `/model/{selector}/invoke-with-response-stream` and body `anthropic_version: bedrock-2023-05-31`; it omits native `model` and `stream` fields. There is no automatic fallback between operations.

Implement `BedrockCredentialProvider` to obtain or refresh AWS credentials through the Host's chosen credential chain. The callback receives scope, region, audience, cancellation and deadline. Return `BedrockCredential::Aws` for SigV4, including a session token and expiry when appropriate. Inference also accepts `BedrockCredential::Bearer`. Metadata inspection requires a separate IAM provider; an inference bearer token is not reused for control-plane calls. Static credential values implement the provider interface for simple Hosts and fixtures. Debug output excludes credential values.

The adapter signs the final URL, headers and serialized payload. It refuses expired credentials and disables redirects and transport retries. Core attempt accounting and retry policy remain authoritative. Cancellation and deadlines stop credential lookup or the active HTTP stream.

## Messages, tools and output

The adapter reuses the [Anthropic Messages protocol](anthropic.md) for system instructions, ordinary tool calls/results, effort/thinking options and signed continuation blocks. Native service tools and unsupported options fail explicitly. AWS binary frames additionally undergo length, CRC, header, event-count and total-byte validation before Messages decoding. Incomplete or corrupt streams cannot complete an attempt.

Register capabilities for the specific model, endpoint and operation. Mantle Messages rejects JSON-schema output; the adapter refuses that combination before HTTP. Runtime structured output still depends on the selected model and operation. A shared codec does not establish service capability or account availability.

## Model and profile inspection

`BedrockInspector` uses `GetFoundationModel` or `GetInferenceProfile`. `BedrockSnapshot` supplies explicit Host evidence linking a model ID to a release; an unknown release stays unverified. Metadata availability alone does not prove inference permission.

Inference profiles are mutable even when their current foundation model has a known release. The inspector records model IDs and a profile revision fingerprint, detects mixed-model targets, and optionally checks `allowed_destination_regions`. This list restricts the destinations advertised by profile metadata; it does not attest where an individual inference executed. `region` is the request/signing origin. To enforce metadata observations, register the inspector with the router and apply its version policy.

## Verification and setup

Local HTTP fixtures cover signing, both stream protocols, malformed frames, cancellation, redirect refusal and model/profile drift. An independently extracted package consumer exercises native Messages. Account-specific inference, model access and destination behavior require separate live verification. See the [test environment guide](env/bedrock.md).

Official contracts: [AWS Messages API](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-messages-api.html), [structured output scope](https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-structured-outputs.html), [GetFoundationModel](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_GetFoundationModel.html), [GetInferenceProfile](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_GetInferenceProfile.html), [Anthropic Bedrock transport](https://github.com/anthropics/anthropic-sdk-python/blob/main/src/anthropic/lib/bedrock/_mantle.py).
