# Changelog

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
