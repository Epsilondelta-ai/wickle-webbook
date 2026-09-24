# 15장. Agent와 RunHandle로 첫 실행 완성

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** AgentBindings에 interruption_policy가 추가되고 생성 시 등록 해석 일부가 신규 start로 지연된다. handle은 영속 segment identity를 갖고 inspect_step/control API가 추가된다.

이어지는 구현: [41장](41-admission.md) · [47장](47-interruptions.md) · [48장](48-controls.md) · [49장](49-inspection.md) · [59장](59-host-migration.md).

[목차](README.md) · [이전 장](14-routing.md) · [다음 장](16-tools.md)

## 이번 장의 출발점과 결과

14장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Agent와 RunHandle로 첫 실행 완성**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/15-agent.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/15-agent.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch17-03-more-futures.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

지금까지의 부품을 Agent facade로 조립한다. Agent는 하나의 scope에 묶이고 start가 수락한 Run은 Host의 실행 driver가 소유한다. RunHandle은 결과와 이벤트를 관찰하고 명시적 취소를 요청하는 손잡이다. UI 화면을 닫는 것과 업무 실행을 취소하는 것은 별개의 결정이다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 15
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. AgentBindings에 state, policy, resolver, router, exchange, clock, ID 등 필요한 Port를 주입한다. create_agent는 설정 검증만 하고 외부 callback을 실행하지 않는다.

2. start에서 현재 정책과 중복 요청을 먼저 확인한다. 신규 요청이면 admission을 원자적으로 저장하고 독립 driver를 시작한다.

3. driver가 lease와 heartbeat를 소유하며 context 준비→model 호출→응답 확정→outcome 저장 순으로 진행하게 한다.

4. RunHandle::outcome과 events는 저장된 상태를 읽는다. drop으로 실행을 취소하지 않고 cancel을 명시적으로 구현한다. 저장 실패 시 성공 문자열을 반환하지 않는다.

## 실제 코드 읽기

`crates/wickle/src/agent.rs`의 이 단계 21–52행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/15-agent.md)에 있다.

```rust
pub trait ModelTokenEstimator: Send + Sync {
    /// Estimate the complete prepared request for its exact route.
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ContractError>;
}

/// Finite runtime bounds, independent of the profile's total execution budgets.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// Lease duration renewed by the detached driver.
    pub lease_ttl_ms: u64,
    /// Renewal interval; at most one third of the lease duration.
    pub heartbeat_interval_ms: u64,
    /// Maximum delay between durable observer polls.
    pub observer_poll_ms: u64,
    /// Maximum events read per page.
    pub event_page_size: usize,
    /// Deadline for admission preparation callbacks, before durable admission.
    pub start_timeout_ms: u64,
    /// Maximum serialized RunRequest bytes.
    pub max_request_bytes: usize,
    /// Reserved output-token limit for the initial text-model call.
    pub max_output_tokens: NonZeroU64,
    /// Model request and response bounds.
    pub response_limits: ModelResponseLimits,
    /// Context byte/item bounds, distinct from token estimates.
    pub projection_limits: ProjectionLimits,
    /// Require a durable StateStore at admission.
    pub require_durable: bool,
}
impl Default for AgentSettings {
    fn default() -> Self {
        Self {
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Agent는 Facade이며 내부 driver는 상태 기계의 실행기다. 복잡한 여러 Port를 사용자에게 한 진입점으로 제공한다. 대가로 AgentBindings가 크고 Host의 조립 코드가 길어진다. 이를 숨기려고 글로벌 singleton을 사용하면 테스트 격리와 여러 tenant 운용이 어려워진다. 생성자에서 I/O를 하지 않는 원칙은 조립 실패와 실행 실패를 분리한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test agent_runtime --locked
python3 "$COURSE/lab.py" check 15 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-state-sqlite/tests/state_store.rs` → `durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen`
- `crates/wickle-state-sqlite/tests/state_store.rs` → `failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit`
- `crates/wickle/tests/agent_runtime.rs` → `starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks`
- `crates/wickle/tests/agent_runtime.rs` → `dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver`

### 결함을 주입하는 연습

start를 한 번 poll한 뒤 Future와 handle을 drop하고 store에서 종료 결과를 확인하라. 같은 request_id로 재시도하고 model call 수를 확인하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

수락된 실행은 관찰자의 수명과 독립되어 완료되어야 한다. 동일 요청은 같은 Run과 저장 결과를 돌려주며 추가 model 호출이 없어야 한다. start를 전혀 poll하지 않았다면 비동기 함수 본문은 아직 실행되지 않았을 수 있다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 15 --dest ../wickle-answer-15
python3 "$COURSE/lab.py" compare 15 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/15-agent.patch"
git apply "$COURSE/solutions/15-agent.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `0d9b104a078e144b78dc3dc5cdd57a06135c982a`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-state-sqlite/src/lib.rs`
- `crates/wickle-state-sqlite/tests/state_store.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/tests/agent_runtime.rs`
- `crates/wickle/tests/budget.rs`
- `crates/wickle/tests/input_binding.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/agent.rs`
- `docs/agents.md`
- `docs/contracts.md`
- `tests/support/agent_consumer.rs`

</details>
