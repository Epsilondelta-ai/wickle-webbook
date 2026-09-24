# Contributing to Wickle

Install [rustup](https://rustup.rs/) and Python 3.9 or later. The repository pins
the development toolchain in `rust-toolchain.toml`; the core supports Rust 1.85.0
or later.

Run these checks from the repository root:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo doc --workspace --no-deps --locked
python3 scripts/check-package.py
```

The package check builds all library archives, extracts them outside the checkout,
and runs focused consumers plus two independent business workspaces. It verifies
model/tool execution, persistence, extension contracts, process recovery and
unchanged core package digests. Use `--allow-dirty` for uncommitted changes.
See [integration validation](docs/integration-validation.md) for the exact scope.

Tests use insertion-ordered JSON objects so digest tests can detect a missing
sorting step. Library-only builds also run separately from test feature unification.

To check the core with the minimum supported Rust version:

```sh
rustup toolchain install 1.85.0 --profile minimal
cargo +1.85.0 check -p wickle --lib --no-default-features --locked
cargo +1.85.0 test -p wickle --locked
RUSTUP_TOOLCHAIN=1.85.0 python3 scripts/check-package.py
```

The core lives in `crates/wickle`. Model, storage, and other adapters belong in
separate crates that depend on the core. Core dependencies are checked against
the allowlist in `scripts/check-package.py`. Test consumers and helpers live
outside the core package under `tests/support`.
