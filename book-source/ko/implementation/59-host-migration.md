# 59장 전체 구현과 변경 검사

[강의](../59-host-migration.md) · [전체 변경 패치](../solutions/59-host-migration.patch)

기준 `1f8841fc027e0c6b9c6274760328333b15320006`. 이 단계에서 바뀐 Rust·manifest·Python 파일의 전체 내용이다. 이전 버전과의 정확한 교체 위치·삭제는 patch를 따른다. 다음 장의 코드와 섞지 않는다.

## `scripts/check-package.py`

```python
#!/usr/bin/env python3
"""Verify the core dependency boundary and consume extracted Cargo packages."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
# Changes to core dependencies require an explicit boundary review.
CORE_DEPENDENCIES = {
    "futures-util", "getrandom", "jsonschema", "serde", "serde_json", "sha2", "thiserror",
    "tokio", "tokio-util",
}


def source_digest(directory):
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(directory).as_posix().encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-dirty", action="store_true")
    runnable = sorted(path.stem.removesuffix("_consumer")
                      for path in (ROOT / "tests/support").glob("*_consumer.rs")
                      if path.name not in {"gather_consumer.rs", "report_process_consumer.rs"})
    parser.add_argument("--consumer", choices=runnable,
                        help="Run base package checks and one consumer; omit for the full suite")
    args = parser.parse_args()
    toolchain = subprocess.check_output(
        ["rustup", "show", "active-toolchain"], cwd=ROOT, text=True
    ).split()[0]
    env = {**os.environ, "RUSTUP_TOOLCHAIN": toolchain}

    def metadata(directory):
        return json.loads(subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            cwd=directory, env=env, text=True,
        ))

    workspace = metadata(ROOT)
    core = next(p for p in workspace["packages"] if p["name"] == "wickle")
    catalog = next(p for p in workspace["packages"] if p["name"] == "wickle-model-router")
    sqlite = next(p for p in workspace["packages"] if p["name"] == "wickle-state-sqlite")
    adapters = next(p for p in workspace["packages"] if p["name"] == "wickle-adapter-runtime")
    openai = next(p for p in workspace["packages"] if p["name"] == "wickle-model-openai")
    responses = next(p for p in workspace["packages"] if p["name"] == "wickle-model-responses")
    azure = next(p for p in workspace["packages"] if p["name"] == "wickle-model-azure-openai")
    anthropic = next(p for p in workspace["packages"] if p["name"] == "wickle-model-anthropic")
    bedrock = next(p for p in workspace["packages"] if p["name"] == "wickle-model-bedrock")
    gemini = next(p for p in workspace["packages"] if p["name"] == "wickle-model-gemini")
    vertex = next(p for p in workspace["packages"] if p["name"] == "wickle-model-vertex")
    xai = next(p for p in workspace["packages"] if p["name"] == "wickle-model-xai")
    mcp = next(p for p in workspace["packages"] if p["name"] == "wickle-mcp")
    sqlite_version = next(d["req"] for d in sqlite["dependencies"] if d["name"] == "rusqlite")
    libraries = [core, catalog, sqlite, adapters, responses, openai, azure, anthropic, bedrock, gemini, vertex, xai, mcp]
    versions = {d["name"]: d["req"] for d in core["dependencies"] if d["kind"] is None}
    for dep in core["dependencies"]:
        if dep["kind"] != "dev":
            if dep["name"] not in CORE_DEPENDENCIES or dep.get("path"):
                raise RuntimeError(f"Unapproved core dependency: {dep['name']}")
    print("Core dependency boundary: passed", flush=True)

    command = ["cargo", "package", "-p", "wickle", "--locked"]
    if args.allow_dirty:
        command.append("--allow-dirty")
    subprocess.run(command, cwd=ROOT, env=env, check=True)
    package_name = f"wickle-{core['version']}"
    archive = Path(workspace["target_directory"]) / "package" / f"{package_name}.crate"
    core_archive_digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    print(f"Package SHA-256: {core_archive_digest}", flush=True)

    with tempfile.TemporaryDirectory(prefix="wickle-consumer-") as temporary:
        base = Path(temporary).resolve()
        if base.is_relative_to(ROOT):
            raise RuntimeError("The consumer must be outside the source repository")
        # The archive is produced by cargo package above, not supplied externally.
        subprocess.run(["tar", "-xzf", str(archive), "-C", str(base)], check=True)
        # Cargo 1.85 resolves unpublished path dependencies through the registry
        # when packaging. Stage the sources so the temporary patch and its lock
        # update cannot alter the checkout. The consumer verifies the archive.
        staged = base / "staged"
        staged.mkdir()
        for filename in ["Cargo.toml", "Cargo.lock"]:
            shutil.copyfile(ROOT / filename, staged / filename)
        for package in libraries:
            manifest = Path(package["manifest_path"])
            destination = staged / manifest.parent.relative_to(ROOT)
            destination.mkdir(parents=True)
            shutil.copyfile(manifest, destination / "Cargo.toml")
            shutil.copyfile(manifest.parent / "LICENSE", destination / "LICENSE")
            shutil.copytree(manifest.parent / "src", destination / "src")
        package_paths = {core["name"]: base / package_name}
        core_source_digest = source_digest(package_paths["wickle"])
        for package in libraries[1:]:
            patches = [argument for name, path in package_paths.items()
                       for argument in ["--config", f'patch.crates-io.{name}.path={json.dumps(str(path))}']]
            subprocess.run(
                ["cargo", "package", "-p", package["name"], "--allow-dirty",
                 "--offline", "--no-verify", *patches],
                cwd=staged, env={**env, "CARGO_TARGET_DIR": str(staged / "target")}, check=True,
            )
            name = f"{package['name']}-{package['version']}"
            packaged = staged / "target/package" / f"{name}.crate"
            print(f"{package['name']} package SHA-256: {hashlib.sha256(packaged.read_bytes()).hexdigest()}", flush=True)
            subprocess.run(["tar", "-xzf", str(packaged), "-C", str(base)], check=True)
            package_paths[package["name"]] = base / name
        consumer = base / "consumer"
        (consumer / "src").mkdir(parents=True)
        library_dependencies = ''.join(
            f'{name} = {{ path = "../{path.name}" }}\n'
            for name, path in package_paths.items()
        )
        (consumer / "Cargo.toml").write_text(
            '[package]\nname = "wickle-package-consumer"\nversion = "0.0.0"\n'
            'edition = "2024"\npublish = false\n\n[workspace]\n\n'
            f'[dependencies]\n{library_dependencies}'
            f'rusqlite = {{ version = "{sqlite_version}", default-features = false, features = ["bundled"] }}\n'
            f'serde_json = "{versions["serde_json"]}"\n'
            f'futures-util = {{ version = "{versions["futures-util"]}", default-features = false, features = ["std", "async-await"] }}\n'
            f'tokio = {{ version = "{versions["tokio"]}", features = ["rt", "macros", "net", "io-util"] }}\n'
            f'\n[patch.crates-io]\n{library_dependencies}',
            encoding="utf-8",
        )
        shutil.copyfile(ROOT / "tests/support/consumer.rs", consumer / "src/main.rs")
        shutil.copyfile(ROOT / "tests/support/mcp_fixture.rs", consumer / "src/mcp_fixture.rs")
        shutil.copyfile(ROOT / "tests/host_contract/delivery.rs", consumer / "src/host_delivery.rs")
        business_entries = {"gather_consumer.rs", "report_process_consumer.rs"}
        examples = sorted(path for path in (ROOT / "tests/support").glob("*_consumer.rs")
                          if path.name not in business_entries)
        if examples:
            (consumer / "src/bin").mkdir()
            for example in examples:
                shutil.copyfile(example, consumer / "src/bin" / example.name)
        # Keep consumer builds independent of the checkout and its build cache.
        env["CARGO_TARGET_DIR"] = str(base / "target")
        # This throwaway consumer does not reuse incremental compiler state.
        env["CARGO_INCREMENTAL"] = "0"
        # Preserve the verified transitive versions when adding the consumer.
        # A fresh lockfile would select newer compatible entries in the local cache.
        shutil.copyfile(ROOT / "Cargo.lock", consumer / "Cargo.lock")
        subprocess.run(["cargo", "metadata", "--format-version", "1", "--offline"],
                       cwd=consumer, env=env, stdout=subprocess.DEVNULL, check=True)
        allowed_registry = {(p["name"], p["version"], p["source"])
                            for p in workspace["packages"] if p["source"] is not None}

        def check_resolution(directory, expected_paths):
            for package in metadata(directory)["packages"]:
                if package["name"] in expected_paths:
                    expected = expected_paths[package["name"]] / "Cargo.toml"
                    if Path(package["manifest_path"]).resolve() != expected:
                        raise RuntimeError(f"Consumer substituted a package: {package['name']}")
                if package["source"] is None:
                    manifest = Path(package["manifest_path"]).resolve()
                    if not manifest.is_relative_to(base):
                        raise RuntimeError(f"Consumer depends on an external path: {manifest}")
                elif (package["name"], package["version"], package["source"]) not in allowed_registry:
                    raise RuntimeError(f"Consumer resolved an unpinned dependency: {package['name']} {package['version']}")

        check_resolution(consumer, package_paths)
        subprocess.run(["cargo", "run", "--locked", "--offline", "--bin", "wickle-package-consumer"],
                       cwd=consumer, env=env, check=True)
        for example in examples:
            if args.consumer and example.stem != f"{args.consumer}_consumer":
                continue
            subprocess.run(["cargo", "run", "--locked", "--offline", "--bin", example.stem],
                           cwd=consumer, env=env, check=True)
        # Two independent application manifests consume the same immutable core
        # archive. Their own business code and selected adapter sets differ.
        if source_digest(package_paths["wickle"]) != core_source_digest:
            raise RuntimeError("Package verification modified the extracted core")
        if args.consumer:
            print(f"Selected packaged consumer: {args.consumer} passed (base contracts and immutable package source verified)", flush=True)
            return
        for scenario, entry, helper, selected in [
            ("gather", "gather_consumer.rs", "source_consumer.rs",
             ["wickle", "wickle-model-router", "wickle-state-sqlite"]),
            ("report", "report_process_consumer.rs", "adapter_consumer.rs",
             ["wickle", "wickle-model-router", "wickle-state-sqlite", "wickle-adapter-runtime"]),
        ]:
            host = base / f"business-{scenario}"
            (host / "src").mkdir(parents=True)
            selected_paths = {name: package_paths[name] for name in selected}
            dependencies = ''.join(f'{name} = {{ path = "../{path.name}" }}\n'
                                   for name, path in selected_paths.items())
            (host / "Cargo.toml").write_text(
                f'[package]\nname = "wickle-{scenario}-host"\nversion = "0.0.0"\n'
                'edition = "2024"\npublish = false\n[workspace]\n[dependencies]\n'
                + dependencies
                + f'serde_json = "{versions["serde_json"]}"\n'
                + f'futures-util = {{ version = "{versions["futures-util"]}", default-features = false, features = ["std", "async-await"] }}\n'
                + f'tokio = {{ version = "{versions["tokio"]}", features = ["rt", "macros", "net", "io-util", "fs"] }}\n'
                + '[patch.crates-io]\n' + dependencies,
                encoding="utf-8",
            )
            shutil.copyfile(ROOT / "tests/support" / entry, host / "src/main.rs")
            shutil.copyfile(ROOT / "tests/support" / helper, host / "src" / helper)
            shutil.copyfile(ROOT / "Cargo.lock", host / "Cargo.lock")
            if source_digest(package_paths["wickle"]) != core_source_digest:
                raise RuntimeError("Business setup modified the extracted core")
            subprocess.run(["cargo", "metadata", "--format-version", "1", "--offline"],
                           cwd=host, env=env, stdout=subprocess.DEVNULL, check=True)
            check_resolution(host, selected_paths)
            subprocess.run(["cargo", "run", "--locked", "--offline"], cwd=host, env=env, check=True)
            if (source_digest(package_paths["wickle"]) != core_source_digest
                    or hashlib.sha256(archive.read_bytes()).hexdigest() != core_archive_digest):
                raise RuntimeError("Business execution modified the core package")
            print(json.dumps({"business_consumer": scenario,
                              "core_archive_sha256": core_archive_digest,
                              "core_source_sha256": core_source_digest,
                              "independent_workspace": True}), flush=True)
    print("Independent package and business consumers: passed (13 libraries; two separate business workspaces using identical core sources)", flush=True)


if __name__ == "__main__":
    main()
```

## `tests/support/agent_consumer.rs`

```rust
// Synthetic adapters and metadata inspector: no provider network calls are made.
// The fixed clock supports deterministic accounting; this does not test timeouts.
use futures_util::{TryStreamExt, stream};
use serde_json::json;
use std::collections::BTreeSet;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::*;
use wickle_model_router::{ModelDispatcherEntry, PolicyModelRouter, RegistryModelDispatcher};
use wickle_state_sqlite::SqliteStateStore;

fn id(s: &str) -> Id {
    Id::new(s).expect("example identifiers")
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
    let mut models = vec![];
    let mut bindings = vec![];
    for name in ["first", "second"] {
        let capabilities = ModelCapabilities {
            revision: id("capabilities"),
            features: BTreeSet::from([id("text")]),
            options_schema: json!({"type":"object","properties":{"reasoning_effort":{"enum":["low","medium","high"]}},"additionalProperties":false}),
            context_window: 4096.try_into().unwrap(),
            max_output_tokens: 512.try_into().unwrap(),
        };
        let model = ModelDefinition {
            model_key: id(name),
            family: id("example"),
            provider: id(name),
            model_id: id("example-model"),
            model_version: id("release-1"),
            version_semantics: VersionSemantics::Pinned,
            lifecycle: ModelLifecycle::Active,
            capabilities: capabilities.clone(),
            evidence: vec![],
        };
        let mut binding = ModelBinding {
            default_options: JsonObject::from([("reasoning_effort".into(), json!("low"))]),
            binding: reference(name),
            model: model.reference(),
            requested_model: model.model_id.clone(),
            adapter: reference("adapter"),
            connection_ref: reference(&format!("{name}-account")),
            target: JsonObject::new(),
            target_schema: json!({"type":"object","additionalProperties":false}),
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("v1"),
            },
            deployment_revision: None,
            version_semantics: VersionSemantics::Pinned,
            capabilities,
            support: ModelSupportStatus::ContractTested,
            evidence: vec![],
        };
        binding.evidence.push(ModelValidationEvidence {
            kind: ModelValidationKind::ContractTest,
            binding_digest: binding.contract_digest(&model)?,
            checked_at_ms: 1000,
            evidence_ref: id("synthetic-fixture"),
            passed: true,
        });
        models.push(model);
        bindings.push(binding);
    }
    RoutingSnapshot::new(
        ModelCatalogSnapshot {
            revision: id("catalog"),
            scope: scope.clone(),
            models,
            bindings,
            aliases: vec![],
        },
        RoutingPolicy {
            revision: id("policy"),
            scope: scope.clone(),
            rules: vec![RoutingRule {
                model_binding: id("primary"),
                purpose: ModelPurpose::Agent,
                primary: reference("first"),
                fallbacks: vec![reference("second")],
                fallback_on: vec![ModelFailureKind::RateLimited],
                version_policy: VersionPolicy::RequirePinned,
                min_support: ModelSupportStatus::ContractTested,
            }],
        },
    )
}
struct TemporaryDatabase(std::path::PathBuf);
impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ExampleClock;
impl Clock for ExampleClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        Ok(ClockReading {
            utc_ms: 1000,
            monotonic_ms: 1000,
        })
    }
    fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
struct ExamplePolicy;
impl PolicyPort for ExamplePolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        _: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if let PolicyAction::InvokeModel { route, .. } = &request.action {
                if route.connection_ref.id == id(&format!("{}-account", route.provider)) {
                    return Ok(PolicyDecision::Allow {});
                }
            }
            Ok(
                if matches!(request.action, PolicyAction::InvokeModel { .. }) {
                    PolicyDecision::Deny {
                        reason: id("unknown-account"),
                    }
                } else {
                    PolicyDecision::Allow {}
                },
            )
        })
    }
}
struct ExampleInspector;
// This echo is a fixture only. A real inspector must read authoritative provider
// metadata instead of presenting requested values as independently observed facts.
impl ModelRouteInspector for ExampleInspector {
    fn inspect<'a>(
        &'a self,
        route: &'a ResolvedModelRoute,
        _: &'a ModelInspectionContext,
    ) -> PortFuture<'a, ModelRouteObservation> {
        Box::pin(async move {
            Ok(ModelRouteObservation {
                route_digest: route.digest(),
                availability: ModelRouteAvailability::Available,
                model_id: Some(route.model_id.clone()),
                model_version: Some(route.model_version.clone()),
                deployment_revision: None,
                version_semantics: VersionSemantics::Pinned,
                evidence_ref: id("synthetic-metadata-check"),
            })
        })
    }
}
struct ExampleModel {
    hold: AtomicBool,
    entered: tokio::sync::Notify,
    route: ResolvedModelRoute,
    calls: AtomicUsize,
    fail: AtomicBool,
}
impl ModelPort for ExampleModel {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: self.route.provider.clone(),
            adapter: self.route.adapter.clone(),
            connection_ref: self.route.connection_ref.clone(),
        }
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        assert_eq!(
            request.options.get("reasoning_effort"),
            Some(&json!("high"))
        );
        assert_eq!(request.route, self.route);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.hold.load(Ordering::SeqCst) {
            self.entered.notify_one();
            return Box::pin(stream::pending());
        }
        let events = if self.fail.load(Ordering::SeqCst) {
            vec![Ok(ModelEvent::ResponseError {
                kind: ModelFailureKind::RateLimited,
                metadata: ModelResponseMetadata::default(),
            })]
        } else {
            vec![
                Ok(ModelEvent::TextDelta {
                    text: format!("{} provider result", self.route.provider),
                }),
                Ok(ModelEvent::ResponseCompleted {
                    finish: ModelFinish::Stop,
                    metadata: ModelResponseMetadata::default(),
                    continuation: vec![],
                }),
            ]
        };
        Box::pin(stream::iter(events))
    }
}
// Application metadata remains separate from the runtime status.
struct MaintenancePolicy;
impl InterruptionPolicy for MaintenancePolicy {
    fn identity(&self) -> VersionedRef { reference("maintenance-policy") }
    fn decide<'a>(&'a self, info: &'a InterruptionInfo) -> PortFuture<'a, InterruptionDecision> {
        Box::pin(async move {
            Ok(if matches!(&info.interruption.cause, InterruptionCause::HostShutdown) {
                InterruptionDecision {
                    action: InterruptionAction::Pause,
                    app_state: Some(AppState { namespace: id("operations"), status: id("maintenance"), metadata: info.configuration.clone() }),
                }
            } else {
                InterruptionDecision { action: InterruptionAction::UseDefault, app_state: None }
            })
        })
    }
}
struct Estimate;
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError> {
        // Conservative test estimate; it is not provider-measured token usage.
        serde_json::to_vec(request)
            .map(|bytes| bytes.len() as u64)
            .map_err(|_| ContractError::new(ErrorCode::InvalidContext, "example.estimate"))
    }
}
fn completed<T>(value: Guarded<T>) -> Result<T, Box<dyn std::error::Error>> {
    match value {
        Guarded::Completed(value) => Ok(value),
        Guarded::ApprovalRequired(_) => Err("unexpected approval".into()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let temporary = TemporaryDatabase(
        std::env::temp_dir().join(format!("wickle-agent-{}", RandomIdSource.next_id()?)),
    );
    std::fs::create_dir(&temporary.0)?;
    let database = temporary.0.join("state.sqlite3");
    let store = Arc::new(SqliteStateStore::open(&database)?);
    let snapshot = routing_snapshot(&scope)?;
    let first = Arc::new(ExampleModel {
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        route: snapshot.route_for_binding(&reference("first"))?,
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(true),
    });
    let second = Arc::new(ExampleModel {
        hold: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        route: snapshot.route_for_binding(&reference("second"))?,
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let policy = Arc::new(PolicyGate::new(
        Arc::new(ExamplePolicy),
        Duration::from_secs(1),
    )?);
    let exchange = Arc::new(
        ModelExchange::with_dispatcher(
            Arc::new(RegistryModelDispatcher::new(vec![
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: first.clone(),
                },
                ModelDispatcherEntry {
                    scope: scope.clone(),
                    port: second.clone(),
                },
            ])?),
            policy.clone(),
        )
        .with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?,
    );
    let profile = AgentProfile::from_json(
        r#"{
        "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
        "name":"Assistant","description":"Agent consumer","instructions":{"text":"Use supplied information"},
        "model_binding":"primary","model_options":{"reasoning_effort":"medium"},"tools":[],"skills":[],"connectors":[],
        "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
        "limits":{"max_model_calls":4,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":10000}
    }"#,
    )?;
    let agent = create_agent(
        profile,
        AgentBindings {
            interruption_policy: Some(InterruptionPolicyBinding {
                policy: Arc::new(MaintenancePolicy),
                configuration: JsonObject::from([("reason".into(), json!("maintenance"))]),
                app_state_schema: Some(AppStateSchema {
                    namespace: id("operations"),
                    schema: json!({"type":"object","properties":{"namespace":{"const":"operations"},"status":{"enum":["maintenance"]},"metadata":{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"],"additionalProperties":false}},"required":["namespace","status","metadata"],"additionalProperties":false}),
                }),
                timeout_ms: 1000.try_into()?,
            }),
            scope: scope.clone(),
            state: store.clone(),
            policy,
            profile_resolver: Arc::new(Catalog),
            model_exchange: exchange,
            router: Arc::new(PolicyModelRouter::new(snapshot)?),
            host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?,
            clock: Arc::new(ExampleClock),
            ids: Arc::new(RandomIdSource),
            tools: None,
            system_input_resolver: None, external_receipt_verifier: None, components: None, context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate),
            settings: AgentSettings {
                max_output_tokens: 128.try_into()?,
                require_durable: true,
                ..AgentSettings::default()
            },
        },
    )?;
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: scope.clone(),
            principal_ref: id("actor"),
            capability_grant_ref: id("grant"),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let request = RunRequest {
        request_id: id("request"),
        session_id: id("session"),
        input: vec![InputContent::Text {
            text: "Retrieve the available result".into(),
        }],
        trigger: RunTrigger::User {},
        model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]),
        max_output_tokens: None,
        output_contract: None,
    };
    let handle = completed(agent.start(request.clone(), context.clone()).await?)?;
    let run_id = handle.run_id().clone();
    let mut events = handle.events(0, context.clone());
    use futures_util::StreamExt;
    let started = events.next().await.ok_or("missing admission event")??;
    assert_eq!(started.event_type, "run.started");
    drop(events);
    let outcome = completed(handle.outcome(&context).await?)?;
    assert_eq!(
        outcome.result,
        OutcomeResult::Succeeded {
            completion_basis: CompletionBasis::TurnEnded
        }
    );
    assert_eq!(
        outcome.output,
        vec![InputContent::Text {
            text: "second provider result".into()
        }]
    );
    assert_eq!(outcome.usage.model_calls, 2);
    assert_eq!(outcome.usage.recovery_attempts, 1);
    let replay = completed(agent.start(request.clone(), context.clone()).await?)?;
    assert_eq!(replay.run_id(), &run_id);
    assert_eq!(completed(replay.outcome(&context).await?)?, outcome);
    let submitted = store.read_execution(&scope, &run_id).await?.submitted.ok_or("submitted identity missing")?;
    submitted.validate(JsonTextLimits::default())?;
    let mut changed = request.clone();
    changed.model_options.insert("reasoning_effort".into(), json!("changed"));
    assert_eq!(agent.start(changed, context.clone()).await.unwrap_err().code, ErrorCode::RequestConflict);
    assert_eq!(store.read_execution(&scope, &run_id).await?.submitted.as_ref().map(|s|s.digest()), Some(submitted.digest()));

    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    let view = completed(agent.get_run(&run_id, &context).await?)?;
    assert_eq!(view.status, RunStatus::Succeeded);
    assert!(!view.deadline_expired);
    let events: Vec<_> = handle
        .events(started.seq.get(), context.clone())
        .try_collect()
        .await?;
    assert_eq!(
        events.last().ok_or("missing finish event")?.event_type,
        "run.finished"
    );
    drop(store);
    let restored = SqliteStateStore::open(&database)?
        .load(&scope, &run_id)
        .await?;
    assert_eq!(restored.snapshot.outcome, Some(outcome));
    assert!(restored.session.active_run_id.is_none());
    // Exercise a cooperative stop against the packaged library and SQLite store.
    first.hold.store(true, Ordering::SeqCst);
    let stopped_request = RunRequest {
        request_id: id("maintenance-request"),
        session_id: id("maintenance-session"),
        ..request
    };
    let stopped = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    tokio::time::timeout(Duration::from_secs(5), first.entered.notified()).await?;
    assert_eq!(completed(stopped.stop_execution(InterruptionCause::HostShutdown, &context).await?)?, ExecutionStopReceipt::Requested);
    let stopped_outcome = completed(tokio::time::timeout(Duration::from_secs(5), stopped.outcome(&context)).await??)?;
    assert!(matches!(&stopped_outcome.result, OutcomeResult::Interrupted { interruption }
        if interruption.cause == InterruptionCause::HostShutdown && interruption.recoverable));
    assert_eq!(stopped_outcome.app_state.as_ref().map(|state| &state.status), Some(&id("maintenance")));
    let reopened = SqliteStateStore::open(&database)?;
    let saved = reopened.load(&scope, stopped.run_id()).await?;
    assert_eq!(saved.snapshot.outcome, Some(stopped_outcome.clone()));
    assert_eq!(saved.session.active_run_id.as_ref(), Some(stopped.run_id()));
    let replay = completed(agent.start(stopped_request.clone(), context.clone()).await?)?;
    assert_eq!(completed(replay.outcome(&context).await?)?, stopped_outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let cancellation = ControlCommand { command_id: id("cancel-maintenance"), principal_ref: context.data.principal_ref.clone(), action: ControlAction::Cancel { reason: id("withdrawn") } };
    let receipt = completed(agent.submit_control_command(stopped.run_id().clone(), cancellation.clone(), context.clone()).await?)?;
    assert!(receipt.processed_segment_id.is_some());
    assert_eq!(completed(stopped.outcome(&context).await?)?, stopped_outcome);
    let latest = completed(agent.start(stopped_request, context.clone()).await?)?;
    assert_eq!(Some(latest.segment_id()), receipt.processed_segment_id.as_ref());
    assert_eq!(completed(latest.outcome(&context).await?)?.result.status(), RunStatus::Cancelled);
    let before_read = SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?;
    assert_eq!(completed(agent.get_control_receipt(stopped.run_id(), &cancellation.command_id, &context).await?)?, receipt);
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, stopped.run_id()).await?, before_read);
    assert_eq!(completed(agent.submit_control_command(stopped.run_id().clone(), cancellation, context.clone()).await?)?, receipt);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    let prepared_id = restored.snapshot.prepared_steps.last().ok_or("missing saved preparation")?.record_id.clone();
    let inspection = completed(agent.inspect_step(&run_id, StepRef::Prepared { record_id: prepared_id }, &context, InspectionOptions::default()).await?)?;
    assert_eq!(inspection.status, InspectionStatus::Found);
    let composition = inspection.composition.as_ref().ok_or("missing composition")?;
    assert_eq!(composition.recorded_run_revision, restored.snapshot.revision);
    assert_eq!(composition.recorded_run_outcome.as_ref().and_then(|outcome| outcome.completion_basis), Some(CompletionBasis::TurnEnded));
    assert!(composition.attempts.iter().any(|attempt| attempt.evidence.contains(&InspectionEvidence::ResponseObserved)));
    assert_eq!(composition.model.as_ref().and_then(|model| model.configuration.as_ref()).and_then(|configuration| configuration.effective.get("reasoning_effort")), Some(&json!("high")));
    assert_eq!(composition.model.as_ref().and_then(|model| model.configuration.as_ref()).and_then(|configuration| configuration.sources.get("reasoning_effort")), Some(&ModelOptionSource::Run));
    let encoded = serde_json::to_value(&inspection)?;
    assert!(encoded["composition"]["model"].get("connection_ref").is_none());
    assert!(encoded["composition"]["model"].get("target").is_none());
    assert_eq!(SqliteStateStore::open(&database)?.load(&scope, &run_id).await?, restored);
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    // Recover a separate cooperatively interrupted interval through the public
    // API. The synthetic primary becomes available after the stop.
    let recover_request = RunRequest { request_id: id("recoverable-request"), session_id: id("recoverable-session"), input: vec![InputContent::Text { text: "Continue after maintenance".into() }], trigger: RunTrigger::User {}, model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]), max_output_tokens: None, output_contract: None };
    let paused = completed(agent.start(recover_request.clone(), context.clone()).await?)?;
    tokio::time::timeout(Duration::from_secs(5), first.entered.notified()).await?;
    completed(paused.stop_execution(InterruptionCause::HostShutdown, &context).await?)?;
    let paused_outcome = completed(paused.outcome(&context).await?)?;
    assert_eq!(paused_outcome.result.status(), RunStatus::Interrupted);
    let checkpoint = SqliteStateStore::open(&database)?.load(&scope, paused.run_id()).await?;
    let source = checkpoint.snapshot.recovery_record(id("maintenance-recovery"))?;
    first.hold.store(false, Ordering::SeqCst);
    first.fail.store(false, Ordering::SeqCst);
    let command = ResumeCommand { run_id: paused.run_id().clone(), expected_revision: checkpoint.snapshot.revision, command_id: id("recover-maintenance"), action: ResumeAction::Recover { recovery_ref: source.reference().clone() } };
    let recovered = completed(agent.resume(command.clone(), context.clone()).await?)?;
    let recovered_outcome = completed(recovered.outcome(&context).await?)?;
    assert_eq!(recovered_outcome.result.status(), RunStatus::Succeeded);
    assert_eq!(recovered_outcome.output, vec![InputContent::Text { text: "first provider result".into() }]);
    assert_ne!(paused.segment_id(), recovered.segment_id());
    assert_eq!(completed(paused.outcome(&context).await?)?, paused_outcome);
    let duplicate = completed(agent.resume(command, context.clone()).await?)?;
    assert_eq!(duplicate.segment_id(), recovered.segment_id());
    assert_eq!(completed(duplicate.outcome(&context).await?)?, recovered_outcome);
    assert_eq!(first.calls.load(Ordering::SeqCst), 4);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    println!(
        "agent consumer: pure construction, detached execution after observer drop, fallback under shared budgets, stored outcome and event replay, duplicate request without new model calls, SQLite reopen, custom interruption state and recovery, immutable prior outcomes, option provenance, durable replay and read-only stored composition inspection"
    );
    Ok(())
}
```

## `tests/support/recovery_consumer.rs`

```rust
// Independent CLI consumer: real SQLite and separate Host processes, synthetic model.
#[allow(dead_code)]
mod host {
    include!("agent_consumer.rs");

    // Crash-boundary verification advances lease time explicitly. A slow disk or
    // a descheduled process must not expire the lease before the intended exit.
    struct RecoveryClock(i64);
    impl Clock for RecoveryClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading { utc_ms: self.0, monotonic_ms: 1000 })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    fn worker(directory: &std::path::Path, mode: &str) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let mut child = std::process::Command::new(std::env::current_exe()?).arg(directory).arg(mode).spawn()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child.try_wait()? { return Ok(status); }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill(); let _ = child.wait();
                return Err("recovery worker exceeded its wall-time watchdog".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    struct ProcessModel {
        inner: ExampleModel,
        directory: std::path::PathBuf,
        interrupt: bool,
    }
    impl ModelPort for ProcessModel {
        fn binding(&self) -> ModelPortBinding { self.inner.binding() }
        fn generate<'a>(&'a self, request: &'a ModelRequest, context: &'a ModelCallContext) -> PortStream<'a, ModelEvent> {
            use std::io::Write;
            let mut calls = std::fs::OpenOptions::new().create(true).append(true).open(self.directory.join("calls")).unwrap();
            writeln!(calls, "{}", context.attempt_id).unwrap();
            calls.sync_all().unwrap();
            std::fs::write(self.directory.join("run"), context.run_id.as_str()).unwrap();
            if self.interrupt { std::process::exit(73); }
            self.inner.generate(request, context)
        }
    }
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args_os().collect();
        if args.len() == 3 {
            return child(std::path::Path::new(&args[1]), args[2] == "interrupt").await;
        }
        let directory = TemporaryDatabase(std::env::temp_dir().join(format!("wickle-recovery-{}", RandomIdSource.next_id()?)));
        std::fs::create_dir(&directory.0)?;
        let first = worker(&directory.0, "interrupt")?;
        assert_eq!(first.code(), Some(73));
        let second = worker(&directory.0, "recover")?;
        assert!(second.success());
        let calls = std::fs::read_to_string(directory.0.join("calls"))?;
        let attempts: Vec<_> = calls.lines().collect();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1]);
        println!("recovery consumer: abrupt Host exit, SQLite reopen under a new owner, original model step retained, two charged physical attempts, accepted command replay without another call (synthetic model, no provider network)");
        Ok(())
    }
    async fn child(directory: &std::path::Path, interrupt: bool) -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope { tenant_id: id("tenant"), workspace_id: id("workspace"), user_id: None };
        let store = Arc::new(SqliteStateStore::open(directory.join("state.sqlite"))?);
        let routing = routing_snapshot(&scope)?;
        let model = Arc::new(ProcessModel { inner: ExampleModel { hold: AtomicBool::new(false), entered: tokio::sync::Notify::new(), route: routing.route_for_binding(&reference("first"))?, calls: AtomicUsize::new(0), fail: AtomicBool::new(false) }, directory: directory.to_owned(), interrupt });
        let policy = Arc::new(PolicyGate::new(Arc::new(ExamplePolicy), Duration::from_secs(1))?);
        let profile = AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Recovery consumer","instructions":{"text":"Use supplied information"},
            "model_binding":"primary","tools":[],"skills":[],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":3,"max_tool_attempts":0,"max_repair_attempts":0,"max_recovery_attempts":2,"max_elapsed_ms":60000}
        }"#)?;
        let mut settings = AgentSettings { require_durable: true, max_output_tokens: 128.try_into()?, ..AgentSettings::default() };
        if interrupt { settings.lease_ttl_ms = 1000; settings.heartbeat_interval_ms = 100; }
        let agent = create_agent(profile, AgentBindings {
            interruption_policy: None,
            scope: scope.clone(), state: store.clone(), policy: policy.clone(), profile_resolver: Arc::new(Catalog),
            model_exchange: Arc::new(ModelExchange::new(model, policy).with_route_inspector(Arc::new(ExampleInspector), Duration::from_secs(1))?),
            router: Arc::new(PolicyModelRouter::new(routing)?), host_instructions: vec!["Preserve the requested output.".into()],
            system_inputs: SystemInputRegistry::new(vec![])?, clock: Arc::new(RecoveryClock(if interrupt { 1000 } else { 3000 })), ids: Arc::new(RandomIdSource),
            tools: None, system_input_resolver: None, external_receipt_verifier: None, components: None,
            context_sources: None, context_token_estimator: None, context_runtime: None, verification: None, skills: None, artifacts: None, hooks: None,
            token_estimator: Arc::new(Estimate), settings,
        })?;
        let context = ExecutionContext::new(ExecutionContextData { scope: scope.clone(), principal_ref: id("actor"), capability_grant_ref: id("grant"), trace_context: None, system_inputs: None }, Default::default());
        if interrupt {
            let request = RunRequest { request_id: id("request"), session_id: id("session"), input: vec![InputContent::Text { text: "Retrieve the available result".into() }], trigger: RunTrigger::User {}, model_options: JsonObject::from([("reasoning_effort".into(), json!("high"))]), max_output_tokens:None,output_contract: None };
            let handle = completed(agent.start(request, context.clone()).await?)?;
            // Regression: wall-time delay exceeds the fixture's short lease.
            std::thread::sleep(Duration::from_millis(1200));
            let outcome = handle.outcome(&context).await;
            return Err(format!("interruption did not occur: {outcome:?}").into());
        }
        let run_id = id(&std::fs::read_to_string(directory.join("run"))?);
        assert_eq!(store.acquire_lease(&scope, &run_id, &id("too-early"), 1000, 1000).await.unwrap_err().code, ErrorCode::LeaseBusy);
        let before = completed(agent.get_run_details(&run_id, &context).await?)?;
        let source = before.recovery_record(RandomIdSource.next_id()?)?;
        let command = ResumeCommand { run_id: run_id.clone(), expected_revision: before.revision, command_id: RandomIdSource.next_id()?, action: ResumeAction::Recover { recovery_ref: source.reference().clone() } };
        let handle = completed(agent.resume(command.clone(), context.clone()).await?)?;
        let result = completed(handle.outcome(&context).await?)?;
        assert_eq!(result.result.status(), RunStatus::Succeeded);
        assert_eq!(result.usage.model_calls, 2);
        assert_eq!(result.usage.recovery_attempts, 1);
        let saved = store.load(&scope, &run_id).await?;
        assert!(matches!(saved.snapshot.model_ledger[0].state, ModelAttemptState::Interrupted { .. }));
        assert_eq!(saved.snapshot.model_ledger[0].model_step_id, saved.snapshot.model_ledger[1].model_step_id);
        let replay = completed(agent.resume(command, context.clone()).await?)?;
        assert_eq!(completed(replay.outcome(&context).await?)?, result);
        Ok(())
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> { host::run().await }
```

## `tests/support/tool_schema_consumer.rs`

```rust
use futures_util::stream;
use serde_json::json;
use std::collections::BTreeMap;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn registry(revision: &str) -> Result<SystemInputRegistry, ContractError> {
    SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id(revision),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
}

fn model_request(tool: ModelTool) -> ModelRequest {
    ModelRequest {
        request_id: id("model-request"),
        purpose: ModelPurpose::Agent,
        route: ResolvedModelRoute {
            binding: reference("local-model"),
            catalog_revision: id("catalog"),
            routing_policy_revision: id("policy"),
            requested_model: id("example-model"),
            model_id: id("example-model"),
            model_version: id("1"),
            version_semantics: VersionSemantics::Pinned,
            provider: id("example-provider"),
            target: JsonObject::new(),
            deployment_revision: None,
            api_contract: ApiContract {
                operation: id("messages"),
                version: id("1"),
            },
            adapter: reference("example-adapter"),
            capability_revision: id("capabilities"),
            connection_ref: reference("connection"),
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Find recent reports".into(),
            }],
        }],
        tools: vec![tool],
        output: ModelOutput::Text {},
        max_output_tokens: 256.try_into().unwrap(),
        options: JsonObject::new(),
        limits: ModelResponseLimits {
            max_input_bytes: 16_384,
            max_response_bytes: 4096,
            max_delta_bytes: 1024,
            max_events: 8,
            max_tool_calls: 1,
        },
    }
}

async fn propose(
    request: &ModelRequest,
    inputs: &JsonObject,
) -> Result<ModelResponse, ModelProtocolError> {
    collect_model_response(
        request,
        Box::pin(stream::iter([
            Ok(ModelEvent::ToolArgumentsDelta {
                index: 0,
                provider_call_id: Some("call".into()),
                name: Some("search_reports".into()),
                delta: serde_json::to_string(inputs).unwrap(),
            }),
            Ok(ModelEvent::ResponseCompleted {
                finish: ModelFinish::ToolCalls,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ])),
    )
    .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = ToolDescriptor::from_json(
        r##"{
      "tool":{"id":"report-search","version":"1"},
      "name":"search_reports","description":"Search reports in the current workspace",
      "input_schema":{
        "type":"object",
        "properties":{
          "query":{"$ref":"#/$defs/Query"},
          "limit":{"type":"integer","minimum":1,"default":10},
          "workspace_id":{"$ref":"#/$defs/WorkspaceId"}
        },
        "required":["query","workspace_id"],"additionalProperties":false,
        "examples":[{"query":"private example","workspace_id":"f7fba7f5-f8e7-4f44-b885-45d0688e9f33"}],
        "$defs":{
          "Query":{"type":"string","minLength":1},
          "WorkspaceId":{"type":"string","format":"uuid"},
          "Unused":{"type":"string","description":"unrelated internal schema"}
        }
      },
      "agent_parameters":["query","limit"],
      "system_bindings":{"workspace_id":"active_workspace_id"},
      "output_schema":{"type":"array","items":{"type":"string"}},
      "side_effect":"read_only","concurrency":"serial","retry":"never","reconcile":false,
      "max_output_bytes":4096
    }"##,
    )?;
    let registry = registry("1")?;
    let compiler = SchemaCompiler::new();
    let compiled = compiler.compile(descriptor, &registry)?;
    let schema = compiled.model_input_schema();
    assert_eq!(schema["required"], json!(["query"]));
    assert_eq!(schema["additionalProperties"], false);
    assert!(schema["properties"].get("workspace_id").is_none());
    assert!(schema.get("examples").is_none());
    assert!(schema["$defs"].get("WorkspaceId").is_none());
    assert!(schema["$defs"].get("Unused").is_none());
    assert!(schema["$defs"].get("Query").is_some());
    assert_eq!(
        compiled.system_bindings()["workspace_id"].key,
        id("active_workspace_id")
    );
    let model_inputs = BTreeMap::from([("query".into(), json!("recent results"))]);
    compiled.validate_model_inputs(&model_inputs)?;
    // Validation alone does not apply defaults; the binder owns that operation.
    let mut full = model_inputs.clone();
    full.insert(
        "workspace_id".into(),
        json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"),
    );
    assert!(compiled.validate_model_inputs(&full).is_err());
    compiled.validate_execution_inputs(&full)?;
    let mut invalid_full = full.clone();
    invalid_full.insert("workspace_id".into(), json!("an-invented-hash"));
    assert!(compiled.validate_execution_inputs(&invalid_full).is_err());
    assert!(compiled.validate_execution_inputs(&model_inputs).is_err());
    let target = ProviderToolTarget {
        model: None,
        provider: id("example-provider"),
        api_contract: ApiContract { operation: id("messages"), version: id("1") },
        capability_revision: id("capabilities"),
    };
    let limits = ProviderToolSchemaLimits::default();
    let projected = CompiledToolContract::compile(&compiled, target.clone(), &NativeToolSchemaCompiler, limits)?;
    let persisted = serde_json::to_string(&projected)?;
    let reopened = CompiledToolContract::restore(&persisted, &compiled, &target, projected.digest(), limits)?;
    let decoded = reopened.decode_arguments(&serde_json::to_string(&model_inputs)?, limits)?;
    compiled.validate_model_inputs(&decoded)?;
    assert_eq!(decoded, model_inputs);
    assert_eq!(reopened.canonical_name(), &id("search_reports"));
    let strict_target = ProviderToolTarget {
        model: Some(reference("example-model")),
        provider: id("openai"),
        api_contract: ApiContract { operation: id("responses"), version: id("v1") },
        capability_revision: id("strict-capabilities"),
    };
    let strict = CompiledToolContract::compile(&compiled, strict_target.clone(), &wickle_model_responses::ResponsesToolSchemaCompiler, limits)?;
    let wire = strict.encode_arguments(&model_inputs)?;
    assert_eq!(wire["limit"]["present"], false);
    let restored_strict = CompiledToolContract::restore(&serde_json::to_string(&strict)?, &compiled, &strict_target, strict.digest(), limits)?;
    let canonical = restored_strict.decode_arguments(&serde_json::to_string(&wire)?, limits)?;
    assert_eq!(canonical, model_inputs);
    let normalized = compiled.normalize_model_inputs(&canonical)?;
    assert_eq!(normalized["limit"], json!(10));
    let mut forged = wire;
    forged.insert("workspace_id".into(), json!("f7fba7f5-f8e7-4f44-b885-45d0688e9f33"));
    assert!(restored_strict.decode_arguments(&serde_json::to_string(&forged)?, limits).is_err());
    let request = model_request(compiled.to_model_tool());
    assert_eq!(
        propose(&request, &model_inputs).await?.tool_calls[0].validation,
        ToolCallValidation::Valid
    );
    assert_eq!(
        propose(&request, &full).await?.tool_calls[0].validation,
        ToolCallValidation::InvalidArguments
    );
    let saved = serde_json::to_string(&compiled)?;
    let restored = compiler.restore(&saved, &registry, compiled.digest())?;
    assert_eq!(restored.digest(), compiled.digest());
    assert_eq!(restored.model_input_schema(), compiled.model_input_schema());
    let changed_registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("active_workspace_id"),
        version: id("2"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])?;
    assert!(
        compiler
            .restore(&saved, &changed_registry, compiled.digest())
            .is_err()
    );
    println!(
        "tool schema consumer: query/limit exposed; hidden schema omitted; hidden input rejected by the model boundary; full UUID schema checked; native and strict provider contracts restored with omission/default semantics; compiled identity preserved and changed registry rejected"
    );
    Ok(())
}
```
