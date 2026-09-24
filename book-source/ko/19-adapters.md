# 19장. 어댑터 조립과 자원 수명

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 중단 settlement·lease 해제·adapter close의 순서와 cleanup window를 구분한다. 원 실행자와 현재 명령 제출자·정리 관찰자를 혼합하지 않는다.

이어지는 구현: [47장](47-interruptions.md) · [48장](48-controls.md).

[목차](README.md) · [이전 장](18-hooks.md) · [다음 장](20-sources.md)

## 이번 장의 출발점과 결과

18장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **어댑터 조립과 자원 수명**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/19-adapters.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/19-adapters.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch15-03-drop.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

등록 metadata를 해석하는 단계와 실제 연결을 여는 단계를 분리한다. ResolvedAssembly는 어떤 버전의 export를 쓸지 고정한 조립도이고 binding set은 한 실행 segment 동안 실제로 열린 자원 집합이다. 대기하면 닫고 재개하면 원래 조립도로 다시 연다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 19
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. AdapterRegistry와 catalog export 등록을 구현하고 이름 충돌·잘못된 버전·connection 누락을 open 전에 검사한다.

2. AdapterFactory::open으로 selected export만 만든다. 반환 descriptor와 저장된 assembly가 일치하는지 검사하고 staging 완료 전에는 공개하지 않는다.

3. 각 export에 scope/run/binding_set_id 수명 검사를 감싼다. component mode와 direct tools/hooks/sources를 동시에 쓰지 못하게 한다.

4. 부분 초기화 실패도 역순으로 close한다. close 하나의 실패가 나머지 정리를 건너뛰게 하지 않는다. Drop은 무효화하고 비동기 정리 완료는 명시적인 close로 확인한다.

## 실제 코드 읽기

`crates/wickle-adapter-runtime/src/runtime.rs`의 이 단계 19–50행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/19-adapters.md)에 있다.

```rust
pub struct AdapterRuntimeSettings {
    /// Maximum time for one factory, further bounded by the initialization deadline.
    pub open_timeout_ms: u64,
    /// Maximum time for one close, further bounded by the total release deadline.
    pub close_timeout_ms: u64,
}
impl Default for AdapterRuntimeSettings {
    fn default() -> Self {
        Self {
            open_timeout_ms: 30_000,
            close_timeout_ms: 1_000,
        }
    }
}

/// Scope-bound reference runtime. It stages all selected instances privately,
/// attests their returned contracts, and publishes one scoped binding set.
#[derive(Clone)]
pub struct AdapterRuntime {
    registry: Arc<AdapterRegistry>,
    state: Arc<dyn StateStore>,
    policy: Arc<PolicyGate>,
    clock: Arc<dyn Clock>,
    settings: AdapterRuntimeSettings,
    failed_cleanup: Arc<Mutex<BTreeMap<(Id, Id), ComponentReleaseReport>>>,
}
impl AdapterRuntime {
    /// Construct without metadata lookup, factory invocation, or external I/O.
    pub fn new(
        registry: Arc<AdapterRegistry>,
        state: Arc<dyn StateStore>,
        policy: Arc<PolicyGate>,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Factory는 생성, Registry는 lookup, wrapper는 Decorator처럼 수명 검사를 추가한다. RAII는 동기 자원 수명에 유용하지만 Rust Drop 안에서 await를 보장할 수 없으므로 비동기 close 프로토콜이 추가된다. 단순 전역 connection pool보다 복잡하지만 다른 scope와 이전 segment의 핸들 오용을 막는다. pool 자체를 없애는 요구는 아니며 Host 내부에서 안전하게 재사용할 수 있다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-adapter-runtime --test runtime_lifecycle --locked
python3 "$COURSE/lab.py" check 19 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-adapter-runtime/tests/agent_components.rs` → `approval_wait_releases_all_instances_and_resume_uses_new_bindings_with_the_frozen_mapping`
- `crates/wickle-adapter-runtime/tests/agent_components.rs` → `initialization_and_dispatch_permissions_remain_separate_for_adapter_exports`
- `crates/wickle-adapter-runtime/tests/hook_exports.rs` → `identical_hook_exports_keep_distinct_selection_policy_and_observation_records`
- `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs` → `an_exact_tool_subset_opens_only_after_admission_and_lease_then_releases_once`

### 결함을 주입하는 연습

세 factory 중 두 번째 open을 실패하게 하라. 첫 번째 자원이 닫히는지, 세 번째가 열리지 않는지 확인하라. 닫힌 export를 보관했다가 다시 호출하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

부분 초기화도 정리되어야 하고 닫힌 참조는 거절된다. Wait 이후 resume은 새 binding_set_id를 사용한다. close 보고는 이미 저장된 outcome과 다른 관찰 대상이다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 19 --dest ../wickle-answer-19
python3 "$COURSE/lab.py" compare 19 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/19-adapters.patch"
git apply "$COURSE/solutions/19-adapters.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `5dcc36182a452fa1549c2fd700c54fd3f0d3293a`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `Cargo.toml`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-adapter-runtime/Cargo.toml`
- `crates/wickle-adapter-runtime/src/lib.rs`
- `crates/wickle-adapter-runtime/src/lifecycle.rs`
- `crates/wickle-adapter-runtime/src/registry.rs`
- `crates/wickle-adapter-runtime/src/runtime.rs`
- `crates/wickle-adapter-runtime/tests/agent_components.rs`
- `crates/wickle-adapter-runtime/tests/hook_exports.rs`
- `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs`
- `crates/wickle-adapter-runtime/tests/runtime_registry.rs`
- `crates/wickle-adapter-runtime/tests/support/mod.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/components.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/hooks.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/tools.rs`
- `crates/wickle/src/component_runtime.rs`
- `crates/wickle/src/hooks.rs`
- `crates/wickle/src/hooks/records.rs`
- `crates/wickle/src/hooks/runtime.rs`
- `crates/wickle/src/input_binding.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/state/hook_state.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/tests/support/agent.rs`
- `docs/adapters.md`
- `docs/agents.md`
- `docs/hooks.md`
- `scripts/check-package.py`
- `tests/support/adapter_consumer.rs`
- `tests/support/agent_consumer.rs`
- `tests/support/hooks_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
