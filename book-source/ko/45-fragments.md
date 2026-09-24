# 45장. 문맥 revision과 파생 자료의 권한 철회

[목차](README.md) · [이전](44-argument-repair.md) · [다음](46-prepared-step.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

44장의 검사를 마친 동일 실습 workspace에서 이어간다. 외부 문서 시스템이 revision을 제공하지 않더라도 코어는 어떤 자료를 사용했는지 알아야 한다. 문서 권한을 철회했는데 과거 답변이나 요약을 통해 같은 정보가 다시 나가면 삭제의 의미가 사라진다.

이번 장의 정확한 checkpoint는 `88477b721e9ce18a82d624fc9dac7633839f7f5b`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/45-fragments.md)와 [정답 patch](solutions/45-fragments.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. fragment identity에 core_revision·content_digest·optional source_revision을 나누어 저장한다. index revision과 item revision도 구분한다.

2. 새 관찰은 같은 내용이어도 core revision이 증가할 수 있다. 같은 입력의 transport retry는 저장된 revision을 재사용한다.

3. Empty/Unavailable은 활성 slot을 교체한다. Deleted는 tombstone으로 과거 파생 자료의 사용도 차단한다.

4. 모델/도구 메시지와 요약에 source lineage와 실제 source_model_request_id를 남기고 auxiliary 모델 호출 전에도 현재 권한을 확인한다.

먼저 `python3 "$COURSE/lab.py" inspect 45`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/context_fragment.rs`의 checkpoint 11행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub enum FragmentOwner {
    /// One immutable-profile conversation.
    Session {
        /// Session identity.
        session_id: Id,
    },
    /// One execution, including its logical model steps.
    Run {
        /// Run identity.
        run_id: Id,
    },
}
/// Stable identity; observation time, batch and external revision are not identity keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FragmentIdentity {
    /// Exact authenticated namespace.
    pub scope: Scope,
    /// Session or Run ownership.
    pub owner: FragmentOwner,
    /// Core-qualified producer selection, including trigger where applicable.
    pub producer_id: Id,
    /// Producer-local fragment identity.
    pub fragment_id: Id,
}
impl FragmentIdentity {
    /// Stable lookup key, independent of the external revision's spelling or ordering.
    pub fn key(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
}
/// Protected fragment content or an explicit selection withdrawal.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Provenance graph와 versioned observation이다. digest는 내용 동일성, revision은 관찰 순서, source revision은 외부 사실이다. lineage를 저장하면 요약의 우회 노출을 막지만 저장/검증 비용이 증가한다. 외부 버전 문자열을 시간순으로 임의 정렬하지 않는다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test context_sources --locked
cargo test -p wickle --test context_projection --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 45 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/context_sources.rs::deletion_in_a_new_run_blocks_previous_run_derived_history`
- `crates/wickle/tests/context_sources.rs::deletion_from_another_trigger_blocks_still_active_run_start_data`
- `crates/wickle/tests/context_sources.rs::reconciled_tool_observation_retains_the_original_model_step_sources`
- `crates/wickle/tests/context_projection.rs::the_latest_tool_round_from_an_earlier_run_is_required_context`
- `crates/wickle/tests/context_projection.rs::an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists`
- `crates/wickle/tests/context_projection.rs::loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority`

### 예측 → 결함 → 복구

A Ready → B Deleted → C Empty 뒤 A에서 파생된 summary를 재사용하도록 시도하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

C가 빈 결과라고 B의 삭제 이력이 사라지는 것은 아니다. 원천 권한을 따라 파생 history도 차단해야 한다. 옛 active 기록에 anchor가 없으면 새 lineage를 추정하지 않고 drain을 요구한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 45 --dest ../wickle-answer-45
python3 "$COURSE/lab.py" compare 45 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/45-fragments.patch"
git apply "$COURSE/solutions/45-fragments.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/context-sources.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
