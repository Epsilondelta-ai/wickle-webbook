# 55장. Vertex 인증과 코어가 소유하는 복구

[목차](README.md) · [이전](54-gemini-schema.md) · [다음](56-xai-contract.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

54장의 검사를 마친 동일 실습 workspace에서 이어간다. Vertex는 Gemini codec을 공유하지만 OAuth·project·location·metadata 계약은 별개다. API key 경로의 테스트 성공을 Vertex 권한 성공으로 승계할 수 없다.

이번 장의 정확한 checkpoint는 `63c5ec65b1c44b6293bfe49e8afaefff32e83c23`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/55-vertex-schema.md)와 [정답 patch](solutions/55-vertex-schema.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. VertexModel에서 공유 Gemini schema compiler를 명시적으로 반환한다.

2. Binding/Profile/Run 옵션 우선순위와 prepared identity를 실제 HTTP body에서 확인한다.

3. 최초 503은 core retry, 잘못된 tool 인자는 ToolRepair로 구분한다. 각각 별도 예약/사용량을 기록한다.

4. OAuth timeout·scope·target·signature/raw fidelity를 유지하면서 remote call 수와 executor 수를 검증한다.

먼저 `python3 "$COURSE/lab.py" inspect 55`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-vertex/src/model.rs`의 checkpoint 12행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct VertexModel {
    connection: VertexConnection,
}
impl VertexModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: VertexConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for VertexModel {
    fn tool_schema_compiler(&self) -> std::sync::Arc<dyn ProviderToolSchemaCompiler> {
        use wickle_model_gemini::protocol::{FunctionSchemaFormat, GeminiToolSchemaCompiler};
        std::sync::Arc::new(GeminiToolSchemaCompiler::new(
            FunctionSchemaFormat::JsonSchema,
        ))
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
            decoder: GenerateContentDecoder::for_vertex(request),
            framing: SseDecoder::new(
                self.connection.0.options.max_transport_bytes,
                self.connection.0.options.max_event_bytes,
                self.connection.0.options.max_protocol_events,
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

합성 기반 재사용이며 authentication은 서비스 경계에 남는다. 테스트 통과의 범위를 정확히 서술하는 것도 engineering 계약이다. local fixture, ADC, gcloud OAuth 경로는 서로 다른 증거다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-model-vertex --test vertex --locked
cargo test -p wickle-model-vertex --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 55 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-model-vertex/tests/vertex.rs::metadata_token_and_http_waits_obey_cancellation_and_deadline`
- `crates/wickle-model-vertex/tests/vertex.rs::minimal_thinking_is_rejected_for_both_flash_releases_before_network`
- `crates/wickle-model-vertex/tests/vertex.rs::two_documented_release_ids_keep_path_and_reported_revision_separate`

### 예측 → 결함 → 복구

live 기록의 실패 원인이 ADC 갱신인지 quota project 권한인지 모델 inference인지 분류하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

인증 준비 실패와 inference 실패를 섞으면 안 된다. 2026-09-24 후속 기록은 gcloud OAuth로 live 확인했지만 release 태그의 지원 행렬을 과거로 돌아가 수정한 것은 아니다. 이 교재 검사는 유료 호출을 새로 수행하지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 55 --dest ../wickle-answer-55
python3 "$COURSE/lab.py" compare 55 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/55-vertex-schema.patch"
git apply "$COURSE/solutions/55-vertex-schema.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/vertex.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
