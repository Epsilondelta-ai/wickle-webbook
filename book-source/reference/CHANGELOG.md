# Changelog

## 0.2.0

Agent runs now preserve submitted request identity, prepared model steps and
execution intervals across retries, interruptions and recovery.

- Compile Tool schemas for each provider while retaining original validation,
  reversible argument encodings and protected system-input binding.
- Configure inference options through Binding, Profile and Run layers, with
  saved provenance and separate settings for verification and compaction.
- Attach validated application state to recoverable interruptions and use
  durable control receipts without rewriting an earlier handle's outcome.
- Inspect saved steps with sensitive values redacted, and observe deadline
  expiry without mutating the Run.
- Preserve scoped context revisions and derived-source permissions, and recover
  SQLite execution boundaries without blindly repeating uncertain effects.
- Use the packaged Host examples individually, including custom interruption
  recovery and native/strict Tool schema decoding.

This release changes Rust integration contracts. Follow the
[migration guide](docs/migration-v0.2.md) before upgrading persistent data;
legacy nonterminal Runs must be resolved with their original runtime.
Install all Wickle crates from the same tag. Provider
[contract and live evidence](docs/model-support.md) remain configuration-specific.

## 0.1.0

Initial MIT-licensed release of the embeddable Rust agent engine.

- Profile-driven serial model/tool loops with scoped policy, system-owned tool inputs and bounded budgets.
- Persisted outcomes, event replay, approval/input/external waits and explicit recovery.
- Versioned model routing and seven separate provider adapters.
- Context sources, Skills, artifacts, lifecycle hooks, context compaction and output verification.
- Reviewed MCP stdio tools, SQLite reference storage and external event Host examples.

See [model validation](docs/model-support.md) for the difference between local
contract checks and limited live smoke evidence. Version 0.1.0 does not promise
a stable API or general production readiness.
