# 18장. Hook 변환과 결과 관찰

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 모델 기본값은 Hook 이전과 이후에 정규화하며 새 Hook output은 정규화 후 저장한다. 과거 Hook 기록은 당시 입력 그대로 재생한다. 파생 자료의 권한을 재검사한다.

이어지는 구현: [44장](44-argument-repair.md) · [45장](45-fragments.md).

[목차](README.md) · [이전 장](17-resume.md) · [다음 장](19-adapters.md)

## 이번 장의 출발점과 결과

17장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Hook 변환과 결과 관찰**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/18-hooks.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/18-hooks.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch13-01-closures.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Hook은 특정 생명주기 지점에 제한된 동작을 끼우는 확장 지점이다. before hook은 문맥 추가나 모델 소유 인자 변환을 할 수 있고 after observer는 이미 확정된 결과를 본다. 관찰 실패가 업무 성공을 실패로 뒤집으면 알림 서버 장애 때문에 결제를 재시도하는 위험이 생긴다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 18
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. HookDefinition에 정확한 위치·버전·우선순위·한도를 담고 동일 우선순위의 안정적 순서를 정한다.

2. before 변환은 허용된 출력만 받고 provenance를 core가 부여한다. system 필드를 바꾸거나 기존 denial을 allow로 바꾸지 못한다.

3. 변환 결과를 저장한 다음 사용한다. 동일 logical model step의 retry와 승인 후 재개는 저장된 변환을 재사용한다.

4. after 결과 보고는 Run outcome과 별도로 저장한다. optional BeforeRun 실패 예외와 다른 변환 위치의 실패 규칙을 구별한다.

## 실제 코드 읽기

`crates/wickle/src/hooks.rs`의 이 단계 16–47행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/18-hooks.md)에 있다.

```rust
pub struct HookDefinition {
    /// Registered identity and exact version.
    pub hook: VersionedRef,
    /// Single permitted lifecycle position.
    pub position: HookPosition,
    /// Lower priorities execute first; IDs break ties.
    pub priority: i32,
    /// Only optional before_run callback failures may continue as warnings.
    pub required: bool,
    /// Positive finite callback timeout, at most one day.
    pub timeout_ms: u64,
    /// Positive byte bound on serialized callback output.
    pub max_output_bytes: usize,
}
impl HookDefinition {
    /// Identity of the complete callback contract, including its bounds.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Reject unbounded callback contracts before admission.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
            || self.max_output_bytes == 0
            || self.max_output_bytes > 16_777_216
        {
            return Err(hook_error(ErrorCode::InvalidContract, "hooks.definition"));
        }
        Ok(())
    }
}

```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

위치별 callback은 Interceptor/Middleware와 닮았고 after hook은 Observer 역할을 한다. 임의 요청을 다음 handler로 넘기는 일반 Chain of Responsibility는 아니다. 엔진이 호출 위치와 합성 규칙을 통제한다. 확장은 편하지만 많아지면 실행 순서가 숨은 의존성이 된다. 버전·순서·변환 기록을 저장하는 비용으로 재현성을 확보한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test agent_hooks --locked
python3 "$COURSE/lab.py" check 18 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/agent_hooks.rs` → `serial_transform_order_uses_priority_then_id_and_keeps_original_model_arguments`
- `crates/wickle/tests/agent_hooks.rs` → `hidden_system_fields_and_invalid_public_values_are_rejected_before_binding_or_execution`
- `crates/wickle/tests/contracts.rs` → `digest_matches_independent_sha256_vectors_and_sorts_nested_objects`
- `crates/wickle/tests/contracts.rs` → `ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized`

### 결함을 주입하는 연습

AfterTool observer가 실패하게 하고 도구 결과와 실행 횟수를 검사하라. BeforeTool에서 숨은 workspace 필드를 주입해 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

observer 실패는 별도 보고에 남고 이미 commit된 효과를 뒤집거나 다시 실행하지 않는다. 숨은 필드 변환은 거절되어야 한다. 보고 저장 전 crash까지 고려하면 callback의 exactly-once 전달을 보장한다고 말할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 18 --dest ../wickle-answer-18
python3 "$COURSE/lab.py" compare 18 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/18-hooks.patch"
git apply "$COURSE/solutions/18-hooks.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `fbe1ed6b258302e472dd90b0d36a87c2c1a43b66`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/hooks.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/tools.rs`
- `crates/wickle/src/context_projection.rs`
- `crates/wickle/src/hooks.rs`
- `crates/wickle/src/hooks/records.rs`
- `crates/wickle/src/hooks/runtime.rs`
- `crates/wickle/src/input_binding.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/state/hook_state.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/tests/agent_hooks.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state_hooks.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/agent_hooks.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/agents.md`
- `docs/hooks.md`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/hooks_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
