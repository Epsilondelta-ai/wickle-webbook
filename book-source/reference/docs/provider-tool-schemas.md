# Provider Tool schema contracts

Compile the canonical Tool with `SchemaCompiler` first. This validates input
ownership and separates model-owned fields from system-owned fields. A provider
compiler receives only the resulting `ModelTool`; it cannot inspect the original
hidden schema or system values through this interface.

```rust,ignore
let target = ProviderToolTarget::for_route(&route);
let limits = ProviderToolSchemaLimits::default();
let projected = CompiledToolContract::compile(
    &canonical_tool, target, &provider_compiler, limits,
)?;
let wire_tool = projected.wire_tool();
let explanations = projected.constraint_fragments();
```

Implement `ProviderToolSchemaCompiler` in the provider adapter. Its immutable
`reference()` identifies the implementation version. Return the supported native
schema and an explicit `ArgumentDecodePlan`; do not remove a Tool because a
provider cannot express a JSON Schema keyword. `NativeToolSchemaCompiler` passes
through the canonical model schema for protocols that support it.

Whenever schema or argument representation changes, the core generates a trusted
explanation containing the complete canonical model schema and the codec rules.
The ordered fragments carry content digests. The contract records original Tool
identity, descriptor/schema digests, provider protocol, selected model/release, capability and compiler
revisions, wire schema, decoding rules and enforcement locations. Size and nesting
bounds apply before model invocation. A request that exceeds those bounds must
fail explicitly rather than omit constraints.

`ArgumentDecodePlan::Fields` maps every exposed canonical field to one unique wire
field. `ArgumentValueEncoding::Presence` represents omission with a false marker
and a required null value placeholder. A true marker denotes a supplied value,
including explicit null. Both envelope members are required, so the codec also
works with providers requiring every wire property. A false marker with a non-null
value, a missing required member, duplicate input keys, or
unknown mapped fields is an argument error. The codec never guesses that null
means omitted and never supplies a system-owned value.

`ArgumentValueEncoding::JsonText` carries shapes that a provider cannot represent
natively. With `optional: false`, a present wire field contains the JSON-encoded
canonical value; the wire schema may still allow that field to be omitted. With
`optional: true`, the wire string contains `[]` for omission or `[value]` for
presence, preserving explicit null.
Invalid JSON, duplicate keys, and precision-losing numbers are argument errors.
The OpenAI adapter selects this representation for nested optional/open objects
and provider schema limits; the original validator remains authoritative.

`ArgumentDecodePlan::JsonObjectText` packs the entire model-owned argument object
into one JSON-string property when a provider cannot represent its root property
count. Decoding requires exactly that outer field and a valid inner object; the
original model schema still rejects missing required fields or unknown keys.
System bindings remain outside the model-owned object.

After decoding, apply the canonical model defaults and validation, run the
before-tool transformation and revalidation, bind system inputs, and validate the
full original execution schema. This contract does not execute or authorize a
Tool. System-dependent cross-field conditions remain in the full execution
validator. Conditions whose truth depends only on exposed fields also remain in
the model projection; hidden branch data is not added to the model explanation.

Persist the entire `CompiledToolContract` only in protected storage. Restore it
with the original `CompiledTool`, the pinned target, and an independently trusted
expected digest. Restoration uses the saved codec and does not run a newer
compiler. Legacy contracts without model identity keep their original digest;
newly prepared contracts pin the model identity as well. Contract format v2
explains only the encodings actually used by its fields. Stored v1 contracts
retain their exact original guidance, digest and codec during restoration; they
are not silently rewritten. Unknown contract formats are rejected. Send only `wire_tool()` and `constraint_fragments()` to the model.

The Agent obtains the compiler through `ModelPort::tool_schema_compiler` and
stores its output in an immutable [prepared model step](prepared-model-steps.md).
It submits the wire schema and explanations, decodes completed calls with the
saved contract, and uses `encode_arguments` for historical calls sent to another
provider. Creating a standalone contract does not change the registered Tool set.

Numeric arguments that cannot round-trip through the current JSON value type without
changing their exact decimal value are rejected with `InvalidArguments` at
`provider_tool.numeric_precision`. Equivalent exponent/decimal spellings are
accepted; large integers or high-precision decimals are never silently rounded.
The historical JSON parser and digest rules remain unchanged.

OpenAI and Azure compiler revision 2 bounds reference-expansion work before building
intermediate schemas. If that budget is exhausted, fields use their reversible
JSON text representation. Previously saved compiler-revision-1 contracts still
restore their own wire schema and codec without invoking revision 2.
