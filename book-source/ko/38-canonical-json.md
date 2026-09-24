# 38장. 숫자를 잃지 않는 JSON과 완료 정책

[목차](README.md) · [이전](37-compatibility.md) · [다음](39-execution-contracts.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

37장의 검사를 마친 동일 실습 workspace에서 이어간다. 요청 ID가 같을 때 제출 내용까지 같은지 판단하려면 JSON 숫자를 먼저 f64로 바꿔 원문을 잃으면 안 된다. 서로 다른 큰 정수가 같은 값으로 반올림되면 중복 요청을 잘못 받아들일 수 있다.

이번 장의 정확한 checkpoint는 `5a849675a58b3f5abf19b971ad943c85949f24e5`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/38-canonical-json.md)와 [정답 patch](solutions/38-canonical-json.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. canonical.rs에 raw JSON text 경계를 만들고 숫자 lexeme를 보존한다. 객체 키는 정렬하지만 배열 순서는 보존한다. decoded key 중복과 trailing data를 검사한다.

2. canonicalize_json_text와 versioned_digest_json에 정규화 버전을 명시한다. 기존 canonical_digest와 sorted-json-v1 기록은 이전 의미대로 유지한다.

3. 입력 byte·중첩 깊이에 유한 한도를 둔다. CompletionPolicy의 turn_end/verified tagged object를 검사하되 기존 올바른 enum을 불필요하게 다시 만들지 않는다.

먼저 `python3 "$COURSE/lab.py" inspect 38`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/canonical.rs`의 checkpoint 16행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub enum CanonicalizationVersion {
    /// Historical Value-based encoding. Existing stored comparisons keep this rule.
    SortedJsonV1,
    /// Object ordering and fixed string escaping with original number lexemes.
    WickleCanonicalJsonV1,
}
use serde::Serialize;

/// Resource bounds applied before allocating the canonical representation.
#[derive(Debug, Clone, Copy)]
pub struct JsonTextLimits {
    /// Maximum input UTF-8 bytes.
    pub max_bytes: usize,
    /// Maximum nested object/array depth (a scalar has depth zero).
    pub max_depth: usize,
}
impl Default for JsonTextLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            max_depth: 128,
        }
    }
}
fn invalid() -> ContractError {
    ContractError::new(ErrorCode::InvalidJson, "$")
}

/// Canonicalize strict JSON, preserving every number token exactly.
///
/// Duplicate keys (including equivalent escaped spellings), trailing JSON,
/// invalid syntax and resource-limit violations are errors. Limits may be lowered;
/// depth is capped at 128 to bound stack use even with untrusted configuration.
/// Output string escaping is serde_json's JSON encoder; Unicode is not normalized.
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Versioned codec과 lossless parsing이다. 전역 arbitrary_precision을 켜서 기존 Value의 의미를 바꾸는 대신 raw_value로 새 경계를 추가한다. 과거 digest 호환을 지키는 장점이 있지만 두 encoder를 구별하고 호출자가 올바른 버전을 지정해야 하는 비용이 있다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test contracts --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 38 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/contracts.rs::success_requires_a_matching_completion_basis_and_verified_success_requires_evidence`
- `crates/wickle/tests/contracts.rs::event_and_input_contracts_reject_unsupported_versions_and_execution_injection`
- `crates/wickle/tests/contracts.rs::route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct`

### 예측 → 결함 → 복구

9007199254740992와 9007199254740993, 1과 1.0, escaped key의 중복을 각각 입력하라. 새 digest를 구 기록 전체에 다시 계산하는 구현을 가정해 보라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

새 text 계약에서는 서로 다른 숫자 원문을 합치지 않는다. 이미 구 serializer가 잃은 숫자 표기를 복구했다고 주장할 수 없다. completion_policy:null, turn_end+verifier_ref, verified에 빈 참조도 오류여야 한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 38 --dest ../wickle-answer-38
python3 "$COURSE/lab.py" compare 38 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/38-canonical-json.patch"
git apply "$COURSE/solutions/38-canonical-json.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/contracts.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
