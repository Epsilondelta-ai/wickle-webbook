# Install Wickle

Wickle is an in-process Rust library. Use Rust 1.85 or later and a Tokio runtime
for asynchronous execution. The repository pins its development compiler in
`rust-toolchain.toml`.

Version 0.2.0 is distributed through GitHub Releases. Add dependencies from the
same Git tag so the core and optional adapters resolve together:

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
```

Select only the adapters your Host uses. Model providers, SQLite persistence,
MCP and adapter assembly live in separate crates. The core has no provider SDK
or database dependency. See [provider configuration](model-providers.md),
[SQLite storage](sqlite-state-store.md) and [MCP tools](mcp.md).

Construct the Host bindings and call the library directly; no separate Wickle
server process is required. See [running an agent](agents.md). Credentials,
connection permissions and configuration loading belong to your Host; the library
does not load `.env` files.

Start with [the runnable Host tutorial](quickstart.md) to select one packaged
consumer. To verify a source checkout in full, run:

```sh
cargo test --workspace --locked
python3 scripts/check-package.py
```

The package check needs Python 3.9 or later. It extracts Cargo archives into a
separate directory and executes consumer applications, including two independent
business workspaces. It does not contact paid model APIs. Additional live-test
setup is described in [provider setup](provider-setup.md); consult the
[model evidence matrix](model-support.md) before relying on a particular release.

Wickle is licensed under [MIT](../LICENSE). The 0.2 API may change in later
releases; existing applications should follow the [migration guide](migration-v0.2.md).

Source archives and checksum verification are described in [release artifacts](releases.md).
