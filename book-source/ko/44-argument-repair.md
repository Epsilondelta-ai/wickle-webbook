# 44장. 인자 복원·기본값·제한된 수정 루프

[목차](README.md) · [이전](43-provider-contracts.md) · [다음](45-fragments.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

43장의 검사를 마친 동일 실습 workspace에서 이어간다. 완전한 모델 응답 안의 잘못된 도구 인자는 수정할 수 있는 업무 제안이다. 깨진 외부 stream envelope와 같은 오류로 취급하면 원문을 잃고 모델이 수정할 기회도 없다.

이번 장의 정확한 checkpoint는 `baac472d2c687f1a6cf976cd768354b808c136ca`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/44-argument-repair.md)와 [정답 patch](solutions/44-argument-repair.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. raw_arguments/provider_arguments에 원본 이름·JSON과 compiler record를 보존한다. 불완전 stream은 여전히 실행 가능한 제안이 아니다.

2. 저장 decode plan으로 canonical 모델 인자를 복원한 뒤 누락된 최상위 direct/local-ref 기본값을 먼저 적용한다. 이 버전은 required 필드에도 선언된 기본값을 적용한다.

3. before_tool 전 검증과 Hook 변환 후 정규화·재검증을 수행한다. 명시 null·중첩 default·system ID는 추정하지 않는다.

4. ToolRepair 예약은 원래 physical response에 연결하고 같은 잘못된 round에서 한 번만 repair budget을 소비한다. 후속 모델 호출은 별도 model budget이다.

먼저 `python3 "$COURSE/lab.py" inspect 44`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/input_binding.rs`의 checkpoint 22행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
    definitions: BTreeMap<Id, SystemInputDefinition>,
}
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

원문과 정규화 표현을 함께 보존하는 layered validation이다. 오류를 고칠 기회를 주지만 무제한 수정 loop를 만들지 않는다. 큰 nested Future는 factory boxing으로 caller poll frame 밖에서 구성한다. Box::pin을 나중에 붙이는 것과 생성 시 stack 사용을 줄이는 것은 다를 수 있다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test input_binding --locked
cargo test -p wickle --test agent_tool_loop --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 44 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/input_binding.rs::direct_required_and_optional_defaults_are_applied_but_conditional_defaults_are_not`
- `crates/wickle/tests/input_binding.rs::an_explicit_nullable_model_value_is_not_replaced_by_its_default`
- `crates/wickle/tests/input_binding.rs::zero_resolver_capacity_and_small_value_bounds_stop_before_unsafe_progress`
- `crates/wickle/tests/agent_tool_loop.rs::cancelling_an_entered_write_retains_its_unknown_effect_and_closes_unstarted_calls`
- `crates/wickle/tests/agent_tool_loop.rs::the_run_deadline_keeps_an_entered_write_unknown_and_closes_the_remaining_plan`
- `crates/wickle/tests/agent_tool_loop.rs::verifier_repair_does_not_repeat_an_applied_business_write`

### 예측 → 결함 → 복구

required limit에 default=10을 선언하고 모델이 생략하게 한다. note=null과 malformed JSON에도 default를 적용해 정상처럼 만들 수 있는가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

유효 객체의 누락 limit는 기본값 10이 된다. null은 생략이 아니고 malformed JSON은 구조부터 실패하므로 default로 살리지 않는다. 모델 인자 오류 단계에서는 Hook·resolver·executor에 외부 효과가 없어야 한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 44 --dest ../wickle-answer-44
python3 "$COURSE/lab.py" compare 44 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/44-argument-repair.patch"
git apply "$COURSE/solutions/44-argument-repair.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/input-binding.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
