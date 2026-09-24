# 60장 전체 구현과 변경 검사

[강의](../60-release-v02.md) · [전체 변경 패치](../solutions/60-release-v02.patch)

기준 `b1416772dd185a3b175db6e3099512d9006f48f5`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `Cargo.toml`

```toml
[workspace]
members = ["crates/wickle", "crates/wickle-model-router", "crates/wickle-state-sqlite", "crates/wickle-adapter-runtime", "crates/wickle-model-openai", "crates/wickle-model-responses", "crates/wickle-model-azure-openai", "crates/wickle-model-anthropic", "crates/wickle-model-bedrock", "crates/wickle-model-gemini", "crates/wickle-model-vertex", "crates/wickle-model-xai", "crates/wickle-mcp"]
default-members = ["crates/wickle"]
resolver = "3"

[workspace.package]
version = "0.2.0"
license = "MIT"
edition = "2024"
rust-version = "1.85"
repository = "https://github.com/Epsilondelta-ai/wickle"

[workspace.dependencies]
getrandom = { version = "=0.3.4", default-features = false }
futures-util = { version = "=0.3.34", default-features = false, features = ["std", "async-await"] }
jsonschema = { version = "=0.56.0", default-features = false }
rusqlite = { version = "=0.40.2", default-features = false, features = ["bundled"] }
reqwest = { version = "=0.13.5", default-features = false, features = ["rustls"] }
serde = { version = "=1.0.229", features = ["derive"] }
serde_json = "=1.0.151"
sha2 = { version = "=0.11.0", default-features = false }
thiserror = "=2.0.20"
tokio = { version = "=1.53.1", features = ["rt", "macros", "sync", "time"] }
tokio-util = { version = "=0.7.19", features = ["rt"] }

[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
```

## `crates/wickle-adapter-runtime/Cargo.toml`

```toml
[package]
name = "wickle-adapter-runtime"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Scoped adapter assembly and lifecycle management for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
futures-util.workspace = true
serde_json.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "test-util"] }
wickle-model-router = { path = "../wickle-model-router", version = "=0.2.0" }
wickle-state-sqlite = { path = "../wickle-state-sqlite", version = "=0.2.0" }

[lints]
workspace = true
```

## `crates/wickle-mcp/Cargo.toml`

```toml
[package]
name = "wickle-mcp"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Reviewed MCP stdio Tool adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]
[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
rmcp = { version = "=0.8.5", features = ["client", "transport-async-rw"] }
serde.workspace = true
serde_json.workspace = true
futures-util = { workspace = true, features = ["sink"] }
tokio = { workspace = true, features = ["process", "io-util"] }
tokio-util = { workspace = true, features = ["codec"] }
[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread"] }
[lints]
workspace = true
```

## `crates/wickle-model-anthropic/Cargo.toml`

```toml
[package]
name = "wickle-model-anthropic"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Anthropic Messages model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
wickle = { path = "../wickle", version = "=0.2.0" }
reqwest.workspace = true
serde_json = { workspace = true, features = ["raw_value"] }
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-azure-openai/Cargo.toml`

```toml
[package]
name = "wickle-model-azure-openai"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Azure OpenAI Responses and deployment metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
reqwest.workspace = true
serde_json.workspace = true
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-bedrock/Cargo.toml`

```toml
[package]
name = "wickle-model-bedrock"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "AWS Bedrock Claude model adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]
[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
wickle-model-anthropic = { path = "../wickle-model-anthropic", version = "=0.2.0" }
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
reqwest.workspace = true
serde_json.workspace = true
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true
aws-sigv4 = { version = "=1.3.0", default-features = false, features = ["sign-http"] }
aws-credential-types = "=1.2.2"
aws-smithy-eventstream = "=0.60.8"
aws-smithy-types = "=1.3.0"
# Bound the signer family to the verified MSRV-compatible API generation.
aws-smithy-runtime-api = "=1.7.4"
aws-smithy-async = "=1.2.5"
aws-smithy-http = "=0.62.0"
bytes = "=1.12.1"
base64 = "=0.22.1"
[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }
[lints]
workspace = true
```

## `crates/wickle-model-gemini/Cargo.toml`

```toml
[package]
name = "wickle-model-gemini"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Google Gemini generateContent model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
wickle = { path = "../wickle", version = "=0.2.0" }
reqwest.workspace = true
serde_json = { workspace = true, features = ["raw_value"] }
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-openai/Cargo.toml`

```toml
[package]
name = "wickle-model-openai"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "OpenAI Responses model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
wickle = { path = "../wickle", version = "=0.2.0" }
reqwest.workspace = true
serde_json.workspace = true
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-responses/Cargo.toml`

```toml
[package]
name = "wickle-model-responses"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Responses request and bounded SSE codecs for Wickle adapters"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
serde_json.workspace = true

[lints]
workspace = true
```

## `crates/wickle-model-router/Cargo.toml`

```toml
[package]
name = "wickle-model-router"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Immutable model catalogs and routing components for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
serde.workspace = true
serde_json.workspace = true

[dev-dependencies]
futures-util.workspace = true
tokio-util.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-vertex/Cargo.toml`

```toml
[package]
name = "wickle-model-vertex"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "Google Cloud Vertex AI Gemini model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-gemini = { path = "../wickle-model-gemini", version = "=0.2.0" }
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
wickle = { path = "../wickle", version = "=0.2.0" }
reqwest.workspace = true
serde_json.workspace = true
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-model-xai/Cargo.toml`

```toml
[package]
name = "wickle-model-xai"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "xAI Responses model and metadata adapters for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle-model-responses = { path = "../wickle-model-responses", version = "=0.2.0" }
wickle = { path = "../wickle", version = "=0.2.0" }
reqwest.workspace = true
serde_json.workspace = true
futures-util.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["net", "io-util", "rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

## `crates/wickle-state-sqlite/Cargo.toml`

```toml
[package]
name = "wickle-state-sqlite"
version.workspace = true
license.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "A durable SQLite state store for Wickle"
publish = false
include = ["Cargo.toml", "LICENSE", "src/**"]

[dependencies]
wickle = { path = "../wickle", version = "=0.2.0" }
rusqlite.workspace = true
serde_json.workspace = true
tokio.workspace = true

[dev-dependencies]
futures-util.workspace = true
tokio-util.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

이 단계는 배포·호환 문서 중심이다. 아래 변경 문서도 원본 기준으로 읽는다.
