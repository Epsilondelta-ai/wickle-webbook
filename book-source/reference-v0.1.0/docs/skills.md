# Load versioned Skills

A Skill is a complete instruction body with a fixed manifest and supporting
artifact references. `SkillRuntime` advertises selected listings in the session
prompt and supplies an explicit loader Tool. It does not execute scripts or
automatically load every body.

## Register manifests and a resolver

Create a `SkillDefinition` with its exact ID/version, name, description, complete
UTF-8 body hash and byte size, support assets, required Tool capabilities, and a
schema for nonsecret profile configuration. `SkillDefinition::hash_body` produces
the expected SHA-256 representation. `definition.metadata()` is the corresponding
Skill metadata for a Host `ProfileResolver`.

Implement the read-only `SkillResolver`:

- `load` receives the pinned definition, selected configuration, current identity,
  cancellation and deadline, and returns the complete body.
- `authorize_use` checks current access to the exact saved version. It must not
  replace the body or implicitly retrieve a newer version.

Build a scoped runtime from existing Host bindings:

```rust
let skills = std::sync::Arc::new(wickle::SkillRuntime::new(
    wickle::SkillBindings {
        scope: bindings.scope.clone(),
        state: bindings.state.clone(),
        policy: bindings.policy.clone(),
        resolver,
        artifacts: bindings.artifacts.clone(),
    },
    definitions,
    wickle::SkillRuntime::catalog_loader(),
    wickle::SkillLimits::default(),
)?);
```

The Host resolver can use `skills.component_metadata(reference)` for registered
Skills and the native loader, while resolving other components through its
existing catalog. Register `skills.loader_tool()` alongside the other Tool
registrations, select `SkillRuntime::catalog_loader()` in `profile.tools`, select
the required `SkillRef`s in `profile.skills`, and set `bindings.skills`.

The model-facing loader is named `skills_load` and accepts `skill_id` and `version`.
Only the exact Skills selected in this Run may be loaded. The core checks required
capabilities against the selected Tools and Tool exports. A Skill's own capability
declaration cannot satisfy its missing Tool dependencies or grant execution rights.

For component mode, register the loader as a catalog Tool in the adapter registry,
or export its descriptor and executor from an adapter factory. Supply the original
export selection when constructing `SkillRuntime`; an alias only changes the
visible Tool name. Keep `AgentBindings.tools` empty in component mode. The Host
owns the resolver's resources and lifetime.

## Preserve complete instructions

Admission pins the full Skill plan and body-free listings. Existing sessions must
retain the same listings and manifest identities. Missing versions or changed
body hashes are errors; the loader does not choose a latest version.

`SkillLimits` bounds each body, the total of distinct active bodies, support-asset
count, and callback time. The loader checks total capacity before fetching another
body. It never truncates a Skill and labels the prefix a successful load. Move
large supporting material into explicitly referenced assets instead.

The registered loader returns `ToolExecutionOutcome::LoadedSkill`. The Tool
boundary rechecks the loader selection and descriptor, actual bound arguments,
scope, Run, call, manifest, size and body hash. Ordinary Tools cannot create this
instruction origin, even by returning a correctly formed body. A loader returning
an ordinary success value without its complete body is rejected.

The protected body and Tool result commit atomically. `ToolResult.skill_ref`
identifies that body for authorized details access; it is excluded from model
observations. The model receives a short load confirmation and the complete
validated instructions as a `Skill` context item with Run lifetime. These
instructions cannot replace Host/Profile System messages or override Tool policy.

## Reuse and authorize

Repeated loads in the same Run reuse the saved body and check current permission.
They remain distinct Tool calls with separate ledger entries and budgets. New Runs
load their selected version again. Duplicate requests and saved Tool settlements
do not rerun the loader.

Current Skill policy, resolver access, and support-artifact access are checked
before local context Hooks and physical model attempts, including retries.
Revocation also blocks a projection containing Hook-derived copies. Historical
Hook validation uses only Skills loaded before that Hook's original model step.

Supporting assets are references. Loading a manifest does not read or execute its
scripts; those operations need separately selected Tools and current permission.
Rust callbacks are trusted Host code, not a sandbox for arbitrary plugins.

The [independent Skill consumer](../tests/support/skills_consumer.rs) loads an
artifact-backed body, uses it through an Agent Tool loop, reopens SQLite, and
checks replay and current access. Its model and metadata inspector are synthetic.
