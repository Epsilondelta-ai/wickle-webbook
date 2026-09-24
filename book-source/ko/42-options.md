# 42장. 모델 옵션의 계층과 단일 재시도 책임

[목차](README.md) · [이전](41-admission.md) · [다음](43-provider-contracts.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

41장의 검사를 마친 동일 실습 workspace에서 이어간다. binding 기본값, Profile 지시, Run별 옵션을 모두 map으로 보내더라도 병합 규칙이 없으면 어떤 값이 실제 사용됐는지 설명할 수 없다. 검증·압축에 agent 옵션을 무조건 복사하는 것도 목적별 모델 계약을 깨뜨린다.

이번 장의 정확한 checkpoint는 `6cf49d071785a0ac67eff8aa7011b16b6fa12c09`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/42-options.md)와 [정답 patch](solutions/42-options.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. Binding → Profile → Run 우선순위를 최상위 키 교체로 구현한다. nested object deep merge는 하지 않는다.

2. ModelConfiguration에 requested/effective, 각 키의 출처, 모델·binding schema revision과 output cap을 저장한다. schema 검증은 둘 다 적용한다.

3. 출력 token cap은 Host/Profile/Run/선택 모델·binding의 한도 중 최솟값으로 정하고 일반 inference option과 구분한다.

4. verification·compaction은 별도 purpose 옵션을 사용한다. SDK retry는 꺼 두고 코어 예약을 거친 physical retry/fallback만 허용한다.

먼저 `python3 "$COURSE/lab.py" inspect 42`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/model_options.rs`의 checkpoint 9행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub enum ModelOptionSource {
    /// Default of the selected physical binding.
    Binding,
    /// Agent profile override.
    Profile,
    /// Caller override for this Run.
    Run,
    /// Explicit verification/compaction configuration, never inherited from the agent.
    Purpose,
}
/// Validated inference settings pinned with a physical model invocation.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfiguration {
    /// Logical overrides after Profile/Run layering, before binding defaults.
    pub requested: JsonObject,
    /// Validated final options sent to the adapter.
    pub effective: JsonObject,
    /// Origin of each effective top-level key.
    pub sources: BTreeMap<String, ModelOptionSource>,
    /// Exact model-ceiling option schema revision.
    pub model_schema_revision: Id,
    /// Exact selected binding option schema revision.
    pub binding_schema_revision: Id,
    /// Host/Profile/Run upper bound before the selected model ceiling.
    pub requested_max_output_tokens: NonZeroU64,
    /// Actual finite per-call output upper bound.
    pub max_output_tokens: NonZeroU64,
}
impl fmt::Debug for ModelConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelConfiguration")
            .field("option_count", &self.effective.len())
            .field("model_schema_revision", &self.model_schema_revision)
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

계층 설정과 provenance snapshot 패턴이다. 각 override 출처를 설명하고 재현할 수 있지만 route 변경 때 새 schema 검증이 필요하다. 단일 retry 책임은 숨은 비용을 막으며 전송 재시도·도구 인자 수정·업무 품질 보완을 별개로 기록한다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test model_execution --locked
cargo test -p wickle-model-router --test routed_execution --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 42 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/model_execution.rs::budget_cancellation_interrupts_policy_even_with_an_independent_caller_token`
- `crates/wickle/tests/model_execution.rs::failed_invocation_or_response_persistence_never_causes_an_untracked_retry`
- `crates/wickle/tests/model_execution.rs::a_completed_ledger_entry_requires_the_exact_typed_response_and_reservation`
- `crates/wickle-model-router/tests/routed_execution.rs::unsettled_or_unknown_tool_effects_block_new_model_steps_and_fallback`
- `crates/wickle-model-router/tests/routed_execution.rs::an_interrupted_physical_attempt_cannot_be_silently_reissued_on_resume`
- `crates/wickle-model-router/tests/routed_execution.rs::agent_calls_cannot_substitute_the_profile_logical_binding`

### 예측 → 결함 → 복구

Binding {a:{x:1,y:2}}, Run {a:{x:3}}를 합친 값과 sources를 예측하라. endpoint나 max_retries를 options에 넣어 보라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

a는 {x:3}으로 교체돼 y가 사라진다. endpoint·credential·transport retry는 inference 옵션이 아니므로 거부한다. fallback은 새로운 대상의 기본값·schema 아래 effective 설정을 다시 확정해야 한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 42 --dest ../wickle-answer-42
python3 "$COURSE/lab.py" compare 42 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/42-options.patch"
git apply "$COURSE/solutions/42-options.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/model-routing.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
