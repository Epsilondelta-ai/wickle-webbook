# 23장. 출력 검증과 제한된 보완 루프

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 검증 목적 옵션과 agent 목적 옵션을 분리한다. candidate 승인·Input 명령·새 segment 수락의 원자성을 확인하고 이전 결과를 다시 쓰지 않는다.

이어지는 구현: [42장](42-options.md) · [45장](45-fragments.md) · [48장](48-controls.md).

[목차](README.md) · [이전 장](22-compaction.md) · [다음 장](24-recovery.md)

## 이번 장의 출발점과 결과

22장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **출력 검증과 제한된 보완 루프**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/23-verification.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/23-verification.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch11-01-writing-tests.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

turn_end는 모델이 유효한 형식으로 턴을 끝냈다는 뜻이고 verified는 등록된 검증 기준을 통과했다는 뜻이다. JSON schema를 만족하는 {"total":-1}도 업무 기준 total>=0에는 실패할 수 있다. 출력 형식과 품질 판정을 분리해야 성공 의미가 명확해진다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 23
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. OutputContract의 text/JSON schema를 locally 검증하고 candidate 원본을 먼저 저장한다.

2. Verifier 정의에 정확한 기준 버전과 구성, evidence boundary를 고정한다. Pass/Revise/Wait/Fail을 상태 전이에 연결한다.

3. Revise 피드백은 Verification origin으로 남기고 repair budget을 차감한다. 다음 모델 호출은 model budget도 별도로 차감한다.

4. 검증 모델 호출은 Verification 목적의 공통 Exchange로 처리한다. 대기 승인은 정확한 candidate에 묶고 verifier 통신 장애와 품질 불합격을 구별한다.

## 실제 코드 읽기

`crates/wickle/src/verification.rs`의 이 단계 11–42행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/23-verification.md)에 있다.

```rust
pub struct OutputSchemaDefinition {
    /// Exact schema identity.
    pub schema_ref: VersionedRef,
    /// JSON Schema, validated without remote reference resolution.
    pub schema: Value,
}
/// Immutable verifier identity and evaluation criteria.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifierDefinition {
    /// Exact implementation/configuration identity.
    pub verifier_ref: VersionedRef,
    /// Exact criteria version recorded with every verdict.
    pub criteria_ref: VersionedRef,
    /// Nonsecret criteria description, pinned with the Run.
    pub criteria: String,
    /// Complete nonsecret runtime configuration, pinned for recovery.
    #[serde(default)]
    pub configuration: JsonObject,
}
/// Candidate saved before invoking the verifier.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationCandidate {
    /// Owning namespace.
    pub scope: Scope,
    /// Owning Run.
    pub run_id: Id,
    /// Agent model step that produced this candidate.
    pub model_step_id: Id,
    /// Exact complete response record, including its route identity.
    pub response_ref: RecordRef,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Verifier는 Strategy이고 보완 루프는 제한된 feedback controller다. CompletionPolicy가 성공의 계약을 명시한다. 별도 검증은 오류를 줄일 수 있지만 비용·지연이 늘고 검증기도 틀릴 수 있다. 업무 진실을 확인해야 한다면 권위 있는 시스템의 읽기 검증이 필요하다. schema-only verifier를 “모든 내용을 검증하는 AI 심사자”로 해석하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test verification --locked
python3 "$COURSE/lab.py" check 23 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-router/tests/routed_execution.rs` → `retry_and_fallback_share_saved_budgets_and_keep_exact_accounts_and_inspection_evidence`
- `crates/wickle-model-router/tests/routed_execution.rs` → `denied_fallback_is_neither_projected_nor_inspected_nor_dispatched`
- `crates/wickle/tests/agent_tool_loop.rs` → `a_missing_compaction_route_is_rejected_before_agent_execution`
- `crates/wickle/tests/agent_tool_loop.rs` → `compaction_preserves_typed_artifact_and_evidence_anchors_from_removed_rounds`

### 결함을 주입하는 연습

형식은 올바르지만 업무 기준에 실패하는 후보를 반환하게 하고 한 번의 repair 뒤 성공시켜라. 그 과정에서 기존 write tool의 실행 횟수를 확인하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

후보와 피드백이 보존되고 repair 1과 추가 모델 attempt가 각각 기록된다. 기존 settled tool을 다시 실행하면 안 된다. 다만 모델이 새로운 call을 제안하면 새로운 업무 행위이므로 별도의 정책과 멱등성 검사가 필요하다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 23 --dest ../wickle-answer-23
python3 "$COURSE/lab.py" compare 23 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/23-verification.patch"
git apply "$COURSE/solutions/23-verification.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `ccffad4af681689b7e799a46dc4a012219bf0747`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-router/tests/routed_execution.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/verification.rs`
- `crates/wickle/src/context_strategy/model_compactor.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/future.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/state/verification_state.rs`
- `crates/wickle/src/verification.rs`
- `crates/wickle/tests/agent_tool_loop.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/mod.rs`
- `crates/wickle/tests/verification.rs`
- `docs/agents.md`
- `docs/verification.md`
- `tests/support/adapter_consumer.rs`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
- `tests/support/compaction_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/hooks_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/skills_consumer.rs`
- `tests/support/source_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_loop_consumer.rs`
- `tests/support/verification_consumer.rs`

</details>
