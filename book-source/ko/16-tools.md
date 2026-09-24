# 16장. 직렬 Tool Loop와 외부 효과 원장

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** Tool 실행은 원래 광고한 ToolSet·compiled contract에 결합한다. raw→decode→default/validation→Hook→재검증→system bind 순서로 바뀐다.

이어지는 구현: [44장](44-argument-repair.md) · [46장](46-prepared-step.md).

[목차](README.md) · [이전 장](15-agent.md) · [다음 장](17-resume.md)

## 이번 장의 출발점과 결과

15장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **직렬 Tool Loop와 외부 효과 원장**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/16-tools.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/16-tools.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch06-02-match.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

모델이 제안한 도구 목록을 먼저 저장하고 원래 순서대로 실행한다. ToolEffect와 ToolResultStatus는 별개다. 결제는 적용됐지만 반환 JSON이 schema를 어길 수 있다. 이때 결과 오류를 NotApplied로 바꾸면 재시도로 중복 결제가 생긴다. Applied/NotApplied/Unknown은 업무 효과에 대한 지식 상태다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 16
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. SerialToolRound에 저장된 plan의 call ID 순회를 구현한다. settled 결과는 재사용하고 unknown 효과가 있으면 진행을 막는다.

2. 각 call에서 descriptor 검증→모델 인자 검증→binding 저장→현재 policy→attempt 예약→dispatch identity 저장 순서를 만든다.

3. executor 진입 직전 권한·취소·시간 경계를 다시 확인한다. 한 executor는 한 물리 시도만 수행하며 숨은 retry를 하지 않는다.

4. observation, receipt, 효과, paired message와 이벤트를 함께 저장한다. 실행 진입 뒤 timeout/panic/취소는 외부 write의 미적용 증거가 아니므로 불확실성을 보존한다.

## 실제 코드 읽기

`crates/wickle/src/tool_execution.rs`의 이 단계 12–43행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/16-tools.md)에 있다.

```rust
pub struct ToolExecutionContext {
    /// Logical call whose plan and bound input were already saved.
    pub call_id: Id,
    /// Charged physical attempt, already recorded before execution.
    pub attempt_id: Id,
    /// Stable external deduplication identity across recovery of this call.
    pub idempotency_key: Id,
    /// Exact authorized namespace.
    pub scope: Scope,
    /// Current authenticated actor.
    pub principal_ref: Id,
    /// Current Host authorization grant.
    pub capability_grant_ref: Id,
    /// Cancelled when the attempt stops, including timeout or caller cancellation.
    pub cancellation: CancellationToken,
    /// Finite execution deadline.
    pub deadline: tokio::time::Instant,
}

/// Effect information attested by the trusted executor, independent of output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// The executor confirms no external business write occurred.
    NotApplied,
    /// An external business write is confirmed; its receipt must be retained.
    Applied,
    /// Whether an external business write occurred could not be established.
    #[default]
    Unknown,
}

```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

영속 Command와 상태 원장 패턴이다. 도구 call은 직렬화 가능한 의도이고 ToolExecutor는 실제 행위다. 이는 단순 GoF Command 객체의 execute 메서드보다 저장·중복 억제 조건을 확장한 형태다. 직렬 처리는 의존 순서와 오류 의미를 단순하게 하는 대신 독립적인 도구도 병렬화하지 못한다. 병렬화는 속도만의 변경이 아니라 승인·효과·취소 의미를 다시 설계하는 일이다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test tool_execution --locked
python3 "$COURSE/lab.py" check 16 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/agent_runtime.rs` → `starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks`
- `crates/wickle/tests/agent_runtime.rs` → `dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver`
- `crates/wickle/tests/agent_tool_loop.rs` → `an_agent_executes_two_calls_then_receives_only_the_safe_observations_and_original_arguments`
- `crates/wickle/tests/agent_tool_loop.rs` → `unknown_invalid_and_denied_calls_return_errors_to_the_model_without_executing`

### 결함을 주입하는 연습

첫 도구가 Applied를 반환하되 출력 schema를 위반하게 하라. 이어지는 도구 전에 unknown 효과도 삽입하라. 기존 도구 재실행과 다음 모델 호출 횟수를 세어라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

첫 도구는 실패 observation이지만 Applied와 receipt가 보존되어 재실행되지 않는다. unknown에서는 후속 도구와 모델이 멈춘다. 모델의 “성공했다”라는 텍스트는 외부 효과의 권위 있는 근거가 아니다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 16 --dest ../wickle-answer-16
python3 "$COURSE/lab.py" compare 16 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/16-tools.patch"
git apply "$COURSE/solutions/16-tools.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `84ad84936e4974d81bd731c6f4bedd445310b8db`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-router/tests/support/routed.rs`
- `crates/wickle-state-sqlite/tests/support/mod.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/tools.rs`
- `crates/wickle/src/context_projection.rs`
- `crates/wickle/src/input_binding.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/message.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/src/views.rs`
- `crates/wickle/tests/agent_runtime.rs`
- `crates/wickle/tests/agent_tool_loop.rs`
- `crates/wickle/tests/budget.rs`
- `crates/wickle/tests/context_projection.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/input_binding.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/tool_execution.rs`
- `crates/wickle/tests/tool_execution.rs`
- `docs/agents.md`
- `docs/contracts.md`
- `tests/support/agent_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
