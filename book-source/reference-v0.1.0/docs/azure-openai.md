# Azure OpenAI Responses adapter

`wickle-model-azure-openai` implements `ModelPort` for resource-level Azure OpenAI
Responses v1 endpoints. The adapter owns HTTP and authentication; the core remains
independent of Azure SDKs, credentials, and environment files.

```rust,ignore
use std::sync::Arc;
use wickle_model_azure_openai::{
    AzureCredential, AzureOpenAiConnection, AzureOpenAiModel, AzureOpenAiOptions,
};

let mut options = AzureOpenAiOptions::new(resource_endpoint, deployment_name);
options.resource_id = Some(arm_account_resource_id);
let connection = AzureOpenAiConnection::new(
    scope.clone(),
    connection_ref,
    Arc::new(AzureCredential::ApiKey(api_key)),
    options,
)?;
let model = Arc::new(AzureOpenAiModel::new(connection.clone()));
```

Register `connection.binding()`, `connection.target()`, and
`AzureOpenAiConnection::api_contract()` in the catalog. The target fixes the
resource origin, deployment name, and optional ARM account resource ID. The route
keeps the underlying model ID and release; only the HTTP request's `model` field is
mapped to the deployment name. The endpoint is `/openai/v1/responses`; this adapter
does not support dated inference APIs or project-level endpoint paths.
[Azure Responses](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/responses).

## Authentication

API keys use `api-key`; Entra tokens use `Authorization: Bearer`. Implement
`AzureCredentialProvider` to obtain or refresh tokens. It is called once per
physical request with the authenticated scope, service audience, cancellation
signal, and effective deadline. The Host supplies correctly scoped tokens and owns
their lifetime. `AzureCredential` itself also implements the trait for explicitly
supplied static credentials; long-lived applications should use a refreshing Host
provider for Entra tokens. The library never reads `.env`, shells out to `az`, or
acquires credentials through an implicit environment chain.

Scope, binding, target, API contract and input validation occur before credential
lookup or HTTP. Redirects and SDK retries are disabled. Cancellation and deadlines
also bound credential refresh. Transport errors and authentication failures do not
cause hidden second requests.

## Deployment metadata and versions

`AzureOpenAiInspector` reads the selected deployment through Azure Resource Manager
using a separate management credential provider. Its protocol version is
`2025-06-01`, independent of Responses v1 and the model release. Configure the ARM
account resource ID as
`/subscriptions/<subscription>/resourceGroups/<group>/providers/Microsoft.CognitiveServices/accounts/<account>`.
The inspector requests that account's selected deployment and checks the response
resource identity before using its model name, version and provisioning state.
[Deployment GET](https://learn.microsoft.com/en-us/rest/api/microsoftfoundry/accountmanagement/deployments/get?view=rest-microsoftfoundry-accountmanagement-2025-06-01).

```rust,ignore
use wickle_model_azure_openai::{AzureInspectionOptions, AzureOpenAiInspector};
let inspector = Arc::new(AzureOpenAiInspector::new(
    connection.clone(),
    management_credentials,
    AzureInspectionOptions::default(),
)?);
```

An inference API key cannot authorize the management API. Missing metadata or
permissions are not replaced with requested versions or assumptions. Observed
model/release changes and the deployment metadata fingerprint can trigger core
route-drift validation. Deployments remain `MutableDeployment`, including those
with automatic upgrades disabled: metadata inspection does not lock the deployment.
Use `VersionPolicy::AllowMutable` only when the Host permits that guarantee. A
policy requiring pinned targets will reject it. Reported response model and token
counts are recorded as returned; an unreported separate version remains absent.

## Responses behavior and verification

The OpenAI and Azure adapters share `wickle-model-responses` for text/JSON,
function calls/results, strict-schema output, bounded SSE parsing, and route-bound
opaque replay. Azure retains its own resource validation, credentials, transport,
and metadata inspection. The [xAI adapter](xai.md) also uses this codec, with
separate reasoning-item validation, replay identity, and usage decoding. OpenAI
and Azure retain their original validation mode. The shared codec does not imply
identical model features; model and binding option schemas must describe each
deployment's supported options.
The adapter explicitly maps `reasoning_effort`, `temperature`, `top_p` and
`verbosity`, and rejects arbitrary option/body overrides. Native provider tools,
stored conversation state, automatic truncation and incomplete executable Tool
plans are disabled.

Local HTTP tests and an extracted-package consumer cover the transport contract.
Live availability and model-release support require checks against configured Azure
deployments; local fixture success is not a claim of live validation. Application
configuration for those checks is described in the [test Host guide](env/azure-openai.md).
