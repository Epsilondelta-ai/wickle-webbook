# 51장. Azure 배포와 strict Tool 계약

[목차](README.md) · [이전](50-openai-schema.md) · [다음](52-anthropic-repair.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

50장의 검사를 마친 동일 실습 workspace에서 이어간다. Azure는 deployment selector와 underlying model version이 다르며 provider schema 한도도 별도다. root property 수가 한도를 넘으면 각 field를 문자열로 바꾸기만 해서는 property 수가 줄지 않는다.

이번 장의 정확한 checkpoint는 `c81215a7a4b4872366b9491ed3cb0751e4610820`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/51-azure-schema.md)와 [정답 patch](solutions/51-azure-schema.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. AzureResponsesToolSchemaCompiler의 대상·depth/property 정책을 별도로 두고 가역 lowering 로직만 공유한다.

2. root 한도 초과에는 JsonObjectText로 모델 소유 전체 object를 하나의 JSON-string field에 담는다.

3. 복원은 정확히 한 outer field와 유효한 inner object를 요구하고 원본 required·hidden field 검증을 다시 적용한다.

4. HTTP fixture에서 field codec과 root-packed codec 둘 다 invalid→repair→단일 업무 실행을 확인한다.

먼저 `python3 "$COURSE/lab.py" inspect 51`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-azure-openai/src/model.rs`의 checkpoint 12행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct AzureOpenAiModel {
    connection: AzureOpenAiConnection,
}
impl AzureOpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: AzureOpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for AzureOpenAiModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        std::sync::Arc::new(wickle_model_responses::AzureResponsesToolSchemaCompiler)
    }
    fn binding(&self) -> ModelPortBinding {
        self.connection.binding()
    }
    fn generate<'a>(
        &'a self,
        request: &'a ModelRequest,
        context: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let state = State {
            connection: &self.connection,
            request,
            context,
            response: None,
            decoder: ResponsesDecoder::new(request, None),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
            ),
            queue: VecDeque::new(),
            started: false,
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

공통 코드 재사용과 provider별 정책 분리의 사례다. OpenAI용 compiler 동작을 Azure 변경에 맞춰 몰래 바꾸면 저장된 계약 의미가 흔들린다. root packing은 가역성은 주지만 provider-side 구조 검증의 일부를 core로 옮긴다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-model-azure-openai --test responses --locked
cargo test -p wickle-model-azure-openai --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 51 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-model-azure-openai/tests/responses.rs::azure_compacts_wide_and_deep_values_without_narrowing_the_canonical_shape`
- `crates/wickle-model-azure-openai/tests/responses.rs::top_level_property_limit_uses_one_json_object_without_exposing_system_fields`
- `crates/wickle-model-azure-openai/tests/responses.rs::empty_object_depth_boundaries_keep_openai_compiler_revision_compatible`

### 예측 → 결함 → 복구

root-packed 값에 extra outer key, inner duplicate key, nonobject, 숨은 workspace를 각각 삽입하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

모두 decode 또는 canonical 검증에서 거절된다. 단순 valid JSON이라는 사실은 유효한 도구 인자임을 뜻하지 않는다. live field codec 통과를 root-packed live 통과로 확대하지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 51 --dest ../wickle-answer-51
python3 "$COURSE/lab.py" compare 51 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/51-azure-schema.patch"
git apply "$COURSE/solutions/51-azure-schema.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/azure-openai.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
