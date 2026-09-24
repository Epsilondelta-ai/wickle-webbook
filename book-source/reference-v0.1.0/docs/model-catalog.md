# Register versioned models

For selection and execution, see [Route and dispatch model calls](model-routing.md).

`wickle` defines model catalog contracts. `wickle-model-router` provides
`ImmutableModelCatalog`, an in-process implementation that a Host can construct
from a validated snapshot. The catalog contains metadata and connection
references. It does not read environment variables, load credentials, discover
models over the network, or call an LLM.

## Model definitions and bindings

| Value | Meaning |
| --- | --- |
| `ModelDefinitionRef` | Exact provider, model key, and model version |
| `ModelDefinition` | Provider model ID, family, version semantics, lifecycle, and model capabilities |
| `ModelBinding` | Exact model definition plus adapter, connection, API contract, target, and deployment revision |
| `ModelCapabilities` | Feature names, option schema, token limits, and metadata revision |
| `ModelAlias` | Explicit provider alias mapped directly to one model definition |
| `ModelCatalogSnapshot` | Definitions, bindings, and aliases under one scope and immutable revision |

Version strings are opaque provider identifiers. Two versions of the same model
ID can coexist, as can bindings for different API operations or deployments.
Adding a provider or version uses string identifiers and registered metadata;
there is no provider enum to extend in the core.

Use a new snapshot revision when changing metadata. A catalog lookup requires
the exact scope and revision. An unknown model version or binding fails; it is
never replaced with another release. An alias points directly to a registered
definition, so alias chains and cycles cannot form a lookup path.

`requested_model` preserves the requested name separately from the definition's
actual model ID and version. Set `version_semantics` from verified metadata.
The presence of a date or the word `latest` in a name does not determine it.
Binding semantics cannot upgrade an alias, mutable deployment, or unverified
definition into a pinned model. A catalog also cannot establish which version a
provider actually served; that belongs to response metadata and live checks.

## Check a candidate

Reading a binding and accepting it for execution are separate operations. A Host
can inspect planned or retired entries. Before selecting a candidate, validate
the returned `ResolvedCatalogBinding` against `CatalogRequirements`.

Validation checks requested features against both the model definition and the
binding. Options must satisfy both schemas. Input and output token budgets must
fit both sets of limits. Targets must satisfy the binding's target schema, whose
provider-specific meaning remains the adapter's responsibility. Unsupported
options are rejected rather than removed.

The requirements also specify a minimum support status and version policy.
Require at least `ContractTested` when accepting execution candidates. A
`Planned` minimum can inspect compatibility while preparing a registration; it
does not establish that the combination has passed a test. Retired or unavailable
models fail candidate validation. `RequirePinned` rejects mutable or unverified candidates;
`AllowMutable` explicitly accepts their weaker version guarantee. Deprecation
metadata remains available to the Host.

Catalog validation does not send options or perform authorization. Routing,
current policy checks, request projection, and adapter wire encoding must use the
same selected combination when making a model call.
Returned DTOs are editable copies. Their `validate` method checks consistency,
not provenance; obtain candidates from the trusted catalog under the pinned
scope and revision instead of accepting a caller-supplied DTO as a catalog result.

## Support evidence and restoration

Support status distinguishes `planned`, `contract_tested`, and `live_verified`.
Validation evidence identifies a successful contract test or live check and the
digest of the exact model, API, adapter, target, and capability combination.
A support declaration without matching successful evidence is rejected. Evidence
for an older deployment or a different API cannot validate a changed binding.

Evidence references point to records maintained by the Host. The catalog checks
their declared identity and consistency; it does not independently authenticate
an external test report or run a live check. Register only evidence produced by
your trusted verification process.

Serialize the snapshot into Host-controlled storage together with its digest.
`ImmutableModelCatalog::restore` validates its contents and compares the expected
digest. Restoring a saved snapshot preserves its alias mapping and versions even
after a newer catalog is constructed.

The [standalone consumer](../tests/support/catalog_consumer.rs) registers two
versions, validates their different capabilities, and restores an older snapshot.
Run all extracted-package consumers with:

```sh
python3 scripts/check-package.py --allow-dirty
```
