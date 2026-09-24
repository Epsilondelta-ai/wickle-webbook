# 43장. 공급자 schema compiler와 가역적 인자 변환

[목차](README.md) · [이전](42-options.md) · [다음](44-argument-repair.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

42장의 검사를 마친 동일 실습 workspace에서 이어간다. 한 제공자가 JSON Schema의 if/then을 지원하지 않는다고 원본 tool을 삭제하거나 제약을 지우면 기능을 잃거나 잘못된 입력이 실행된다. 제공자 표현과 실행의 최종 계약을 분리해야 한다.

이번 장의 정확한 checkpoint는 `594eb25d17683d185a4ad51cf8513d3ff9aad2e0`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/43-provider-contracts.md)와 [정답 patch](solutions/43-provider-contracts.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. 기존 SchemaCompiler로 model-owned와 system-owned를 먼저 분리한다. ProviderToolSchemaCompiler에는 ModelTool만 전달한다.

2. CompiledToolContract에 wire schema, constraint fragments, decode plan, 원본 identity·digest, provider/API/model/capability/compiler revision을 고정한다.

3. Identity·Fields·Presence로 표현을 복원한다. Presence는 생략과 명시 null을 구분하며 false에 non-null payload를 허용하지 않는다.

4. 복원은 저장된 schema·codec으로 수행한다. 현재 compiler를 재실행하지 않는다. size/depth와 필드 소유권을 검사한다.

먼저 `python3 "$COURSE/lab.py" inspect 43`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/provider_tool_schema.rs`의 checkpoint 13행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct ProviderToolTarget {
    /// Provider namespace, including deployment-specific provider adapters.
    pub provider: Id,
    /// Exact operation and API version.
    pub api_contract: ApiContract,
    /// Pinned target capability revision.
    pub capability_revision: Id,
}
/// Finite bounds on compilation, persisted projection and incoming arguments.
#[derive(Debug, Clone, Copy)]
pub struct ProviderToolSchemaLimits {
    /// Maximum serialized canonical or wire schema/tool bytes.
    pub max_schema_bytes: usize,
    /// Maximum schema nesting before traversal or serialization.
    pub max_schema_depth: usize,
    /// Maximum total serialized compiled contract bytes, including explanations.
    pub max_contract_bytes: usize,
    /// Maximum provider argument bytes before parsing.
    pub max_argument_bytes: usize,
}
impl Default for ProviderToolSchemaLimits {
    fn default() -> Self {
        Self {
            max_schema_bytes: 65_536,
            max_schema_depth: 64,
            max_contract_bytes: 262_144,
            max_argument_bytes: 65_536,
        }
    }
}
/// Reversible representation of a single model-owned field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArgumentValueEncoding {
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 생략과 null을 손으로 복원해 보기

아래는 실제 `provider_tool_schema.rs` 테스트의 `Restricted` compiler 예시다. 모든 provider가 같은 field 이름을 쓴다는 뜻은 아니다. decode plan이 `q→query`, `n→note`, presence key `present`, payload key `value`를 지정했다.

| wire 인자 | canonical 모델 인자 | 결과 |
| --- | --- | --- |
| `{"q":"hi","n":{"present":false,"value":null}}` | `{"query":"hi"}` | note 생략 |
| `{"q":"hi","n":{"present":true,"value":null}}` | `{"query":"hi","note":null}` | 명시 null |
| `{"q":"hi","n":{"present":true,"value":"memo"}}` | `{"query":"hi","note":"memo"}` | 명시 문자열 |
| `{"q":"hi","n":{"present":false,"value":"memo"}}` | 없음 | 모순된 envelope 거절 |

원래 query의 최소 길이 같은 provider schema에 담기지 않은 제약도 canonical validator가 검사한다. 숨은 workspace는 이 표 어디에도 들어가지 않고 뒤의 system binding에서만 공급한다. 이 예를 그대로 full execution schema라고 해석하지 않는다.

## 설계 이유·패턴·장단점

Compiler와 Anti-Corruption Layer이다. native 제약은 최대한 사용하고 남은 제약은 모델 문맥에 설명하되 실제 실행 전에 원본 validator가 최종 권위를 가진다. 텍스트 설명은 provider 강제 검증과 같은 보장이 아니다. 추가 문맥은 token·byte 비용을 늘린다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test provider_tool_schema --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 43 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/provider_tool_schema.rs::native_projection_restores_exactly_and_rejects_changed_destination_or_projection`
- `crates/wickle/tests/provider_tool_schema.rs::compiler_output_cannot_change_ownership_drop_fields_or_ambiguate_presence`
- `crates/wickle/tests/provider_tool_schema.rs::codecs_never_silently_round_numeric_arguments`

### 예측 → 결함 → 복구

모델에 공개한 optional note가 생략된 경우와 명시 null인 경우를 같은 wire null로 바꾸는 codec을 생각해 보라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

두 경우의 의미를 잃으므로 허용할 수 없다. 필드 mapping은 일대일이어야 하며 숨은 system field는 schema뿐 아니라 추가 설명에서도 제외해야 한다. Native pass-through가 적합한 provider에 불필요한 strict 층을 넣지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 43 --dest ../wickle-answer-43
python3 "$COURSE/lab.py" compare 43 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/43-provider-contracts.patch"
git apply "$COURSE/solutions/43-provider-contracts.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/provider-tool-schemas.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
