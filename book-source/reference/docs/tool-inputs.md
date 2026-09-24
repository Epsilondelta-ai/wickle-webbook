# Compile tool input schemas

A tool definition declares its full execution schema and an explicit
`agent_parameters` list. `SchemaCompiler` derives the model schema from that list.
System-owned fields are supplied by Host code through registered input definitions.
The compiler validates these definitions without fetching their runtime values.

| Parameter | Model may supply it | Source |
| --- | --- | --- |
| `query` | Yes | Model input |
| `limit` | Yes | Model input; its declared default remains in the schema |
| `workspace_id` | No | Registered `active_workspace_id` system input |

The descriptor uses `agent_parameters: ["query", "limit"]` and
`system_bindings: {"workspace_id": "active_workspace_id"}`. Without an alias,
a hidden parameter uses the registered system key with the same name. Extra
system keys do not become tool arguments. Compilation does not apply defaults or
invent missing identifiers; those values are checked at the binding boundary.

## Register and compile

```rust
use serde_json::json;
use wickle::{Id, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource};

let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
    key: Id::new("active_workspace_id")?,
    version: Id::new("1")?,
    value_schema: json!({"type":"string", "format":"uuid"}),
    source: SystemInputSource::Run {},
}])?;
let compiler = SchemaCompiler::new();
let compiled = compiler.compile(descriptor, &registry)?;
let model_tool = compiled.to_model_tool();
```

`descriptor` is a `ToolDescriptor`, decoded with `ToolDescriptor::from_json` or
constructed by trusted Host code. Model-facing tool names use 1–64 ASCII letters,
digits, underscores, or hyphens. Its complete execution schema, explicit input
list, version, and execution policy are separate from `model_tool`, which contains
only the model-facing name, description, and derived schema. The
[standalone consumer](../tests/support/tool_schema_consumer.rs) contains a complete
runnable descriptor and demonstrates validation through the model response boundary.

`agent_parameters` is required. An empty list exposes no arguments; listing every
property makes every argument model-owned. Unknown or duplicate names and aliases
that target a model-owned parameter are rejected. Hidden keys must resolve to one
registered system definition. Definitions may identify either an immutable run
input or a versioned Host resolver; registration does not execute the resolver.

The model schema contains selected properties, their required-field intersection,
and `additionalProperties: false`. Even a correct system UUID is rejected when
submitted through the model input. The full execution schema independently checks
all required fields and formats. UUID format validity alone does not establish
that a resource exists or belongs to the caller.

## Supported projection shapes

Input ownership applies to whole top-level parameters. Selecting an object
parameter exposes that entire object; an internal `_id` field does not change its
ownership. Parameter names and system aliases are exact map keys, with no object
path evaluation.

The compiler preserves selected property constraints and annotations. It removes
root annotations such as defaults and examples and includes only reachable local
definitions. The supported reference forms are `#/$defs/<name>` and
`#/definitions/<name>`. Unsupported subpaths, external or dynamic references,
identifier/anchor scopes, and nested definition containers are rejected explicitly.

For a schema containing hidden fields, unsupported root conditions that relate
multiple fields are rejected instead of being silently removed. When every field
is model-owned, supported original root conditions remain in the projection.
Conditions entirely inside a selected object parameter remain model-owned.

Actual value capture, lookup, and persistence are described in
[Bind system tool inputs](input-binding.md).

## Preserve a compiled definition

`CompiledTool` owns its validated descriptor, selected system definitions, and
read-only derived schema. Its digests identify the descriptor, model schema,
bindings, and compiler version. Serialize it only into Host-controlled or protected
storage; the complete definition includes schemas that are hidden from the model.

Restore with `compiler.restore(serialized, &registry, expected_digest)`. Restoration
recompiles and compares the derived data and pinned digest. A changed system input
revision or altered model schema is rejected, even when the tool name is unchanged.
