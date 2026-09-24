# 26장. Azure 배포·인증과 공통 Responses codec

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** Azure 정책 한도와 underlying model identity를 분리한다. root field 수 초과는 JsonObjectText, compiler revision 2는 공유 확장 budget을 사용한다.

이어지는 구현: [51장](51-azure-schema.md) · [58장](58-recovery-audit.md).

[목차](README.md) · [이전 장](25-openai.md) · [다음 장](27-anthropic.md)

## 이번 장의 출발점과 결과

25장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Azure 배포·인증과 공통 Responses codec**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/26-azure.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/26-azure.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch07-02-defining-modules-to-control-scope-and-privacy.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Azure는 같은 Responses 계열이어도 deployment selector, 실제 모델 release, credential audience가 별개다. OpenAI 경로의 host 이름만 바꾸는 것으로 동일하게 취급할 수 없다. 공통 wire codec은 재사용하되 연결·인증·메타데이터 관찰은 분리한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 26
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. OpenAI 전용 위치에 있던 공통 Responses codec/SSE/response 코드를 wickle-model-responses로 추출하고 OpenAI adapter의 import를 갱신한다.

2. Azure connection과 Host token provider를 구현하고 각 물리 요청의 제한 시간 안에서 토큰을 얻는다.

3. deployment와 reported model identity를 분리해 encode/decode한다. ARM inspection은 별도의 target·권한·audience를 확인한다.

4. 모델 업그레이드 drift와 unknown metadata를 구별하고 잘못된 scope/audience는 credential 조회 또는 HTTP 이전에 거절한다.

## 실제 코드 읽기

`crates/wickle-model-azure-openai/src/model.rs`의 이 단계 12–43행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/26-azure.md)에 있다.

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
            finished: false,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

실제 두 번째 사용처가 생긴 뒤 공통 모듈을 추출하는 점이 중요하다. DRY는 모든 제공자를 하나의 거대 adapter로 합치라는 뜻이 아니다. 공통 프로토콜만 추출하고 서비스 정책은 각 Adapter에 둔다. 공유 코드 수정은 여러 provider에 영향을 주므로 양쪽 회귀 검사가 필요하다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-azure-openai --test responses --locked
python3 "$COURSE/lab.py" check 26 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-azure-openai/tests/responses.rs` → `deployment_mapping_preserves_model_identity_and_refreshes_entra_per_request`
- `crates/wickle-model-azure-openai/tests/responses.rs` → `wrong_scope_route_api_and_credentials_never_make_an_http_request`

### 결함을 주입하는 연습

deployment 이름은 그대로 두고 관찰된 모델 버전만 바꿔라. OpenAI용 credential binding을 Azure route에 넣어라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

같은 deployment 문자열도 동일 release 보장이 아니므로 drift를 검출해야 한다. credential 연결은 정확한 provider/adapter/revision에 맞아야 한다. 공유 codec을 쓴다는 사실은 인증 계약까지 같다는 뜻이 아니다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 26 --dest ../wickle-answer-26
python3 "$COURSE/lab.py" compare 26 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/26-azure.patch"
git apply "$COURSE/solutions/26-azure.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `0f63057ed587f2f456dc2e92bfd585036164220a`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `.env.example`
- `Cargo.lock`
- `Cargo.toml`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-model-azure-openai/Cargo.toml`
- `crates/wickle-model-azure-openai/src/auth.rs`
- `crates/wickle-model-azure-openai/src/connection.rs`
- `crates/wickle-model-azure-openai/src/inspection.rs`
- `crates/wickle-model-azure-openai/src/lib.rs`
- `crates/wickle-model-azure-openai/src/model.rs`
- `crates/wickle-model-azure-openai/tests/responses.rs`
- `crates/wickle-model-azure-openai/tests/support/mod.rs`
- `crates/wickle-model-openai/Cargo.toml`
- `crates/wickle-model-openai/src/lib.rs`
- `crates/wickle-model-openai/src/model.rs`
- `crates/wickle-model-openai/tests/support/mod.rs`
- `crates/wickle-model-responses/Cargo.toml`
- `crates/wickle-model-responses/src/codec.rs`
- `crates/wickle-model-responses/src/lib.rs`
- `crates/wickle-model-responses/src/response.rs`
- `crates/wickle-model-responses/src/sse.rs`
- `docs/azure-openai.md`
- `docs/env/azure-openai.md`
- `scripts/check-package.py`
- `tests/support/azure_consumer.rs`
- `tests/support/model_http.rs`

</details>
