# 54장. Gemini schema 변환과 JSON bytes 전송

[목차](README.md) · [이전](53-bedrock-repair.md) · [다음](55-vertex-schema.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

53장의 검사를 마친 동일 실습 workspace에서 이어간다. Gemini의 지원 schema 형태와 서명/원문 replay를 동시에 지켜야 한다. precision을 보존한 JSON을 Value로 다시 만들고 재직렬화하면 숫자 원문이 손실될 수 있다.

이번 장의 정확한 checkpoint는 `da7c0537883b0bb8c9cb18c34a760176bbe9430d`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/54-gemini-schema.md)와 [정답 patch](solutions/54-gemini-schema.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. Gemini 전용 schema compiler로 API v1/v1beta 표현 차이를 적용하고 남은 제약은 canonical 문맥/검증에 보존한다.

2. signature와 invalid raw 인자의 원래 순서를 유지하고 replay 위변조를 검사한다.

3. encode_request와 encode_vertex_request가 Vec<u8>를 반환하도록 연결한다. HTTP body에 그 bytes를 그대로 넣는다.

4. 현재 model/option/route scope 검사와 source 권한을 유지한 상태에서 실제 loopback HTTP 요청을 확인한다.

먼저 `python3 "$COURSE/lab.py" inspect 54`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-gemini/src/schema.rs`의 checkpoint 9행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct GeminiToolSchemaCompiler {
    format: FunctionSchemaFormat,
}
impl GeminiToolSchemaCompiler {
    /// Select an explicit endpoint dialect without changing the requested API version.
    pub fn new(format: FunctionSchemaFormat) -> Self {
        Self { format }
    }
}
impl ProviderToolSchemaCompiler for GeminiToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new(match self.format {
                FunctionSchemaFormat::OpenApi => "wickle-gemini-openapi-schema",
                FunctionSchemaFormat::JsonSchema => "wickle-gemini-json-schema",
            })
            .expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.api_contract.operation.as_str() != "stream_generate_content"
            || !matches!(
                (
                    target.provider.as_str(),
                    target.api_contract.version.as_str(),
                    self.format
                ),
                ("google-gemini", "v1", FunctionSchemaFormat::OpenApi)
                    | ("google-gemini", "v1beta", FunctionSchemaFormat::JsonSchema)
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

문자열·JSON AST·HTTP bytes는 서로 다른 표현 경계다. 직렬화된 bytes를 다시 JSON으로 인코딩하면 JSON object가 아닌 byte 배열을 보내게 된다. raw fidelity를 위해 API 반환형이 달라지므로 직접 codec 소비자도 이관해야 한다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-model-gemini --test generate --locked
cargo test -p wickle-model-gemini --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 54 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-model-gemini/tests/generate.rs::root_constraints_and_reference_siblings_survive_projection_without_overwriting_intersections`
- `crates/wickle-model-gemini/tests/generate.rs::compiler_rejects_a_dialect_that_does_not_match_the_saved_destination`
- `crates/wickle-model-gemini/tests/generate.rs::compact_branching_references_exhaust_a_shared_budget_and_fall_back_without_expanding`

### 예측 → 결함 → 복구

encode_request 결과를 HTTP client의 json(...) 인자로 보내는 결함을 넣어 body를 캡처하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

byte 배열을 JSON 숫자 배열로 직렬화하는 잘못된 body가 드러난다. 직접 body(...)로 전송해야 한다. 요청 모델 이름과 실제 관찰 revision은 따로 유지한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 54 --dest ../wickle-answer-54
python3 "$COURSE/lab.py" compare 54 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/54-gemini-schema.patch"
git apply "$COURSE/solutions/54-gemini-schema.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/gemini.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
