# 25장. OpenAI Responses 어댑터

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** strict provider schema를 가역 compiler로 생성하고 추가 제약을 설명한다. 완전 invalid args는 repair 대상으로 보존하며 최종 Responses compiler revision 2는 expansion work를 제한한다.

이어지는 구현: [50장](50-openai-schema.md) · [58장](58-recovery-audit.md).

[목차](README.md) · [이전 장](24-recovery.md) · [다음 장](26-azure.md)

## 이번 장의 출발점과 결과

24장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **OpenAI Responses 어댑터**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/25-openai.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/25-openai.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch17-02-concurrency-with-async.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

여기서부터 실제 wire protocol을 공통 ModelEvent로 바꾼다. 먼저 OpenAI Responses 경로를 구현한다. 로컬 HTTP fixture가 요청 body와 SSE 조각을 통제하므로 API key 없이도 codec과 취소를 검사할 수 있다. fixture 성공은 실제 계정에서 특정 모델이 사용 가능하다는 증거가 아니다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 25
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. Connection에 scope·credential binding·명시적 endpoint 계약을 둔다. 생성자에서 모델을 호출하지 않는다.

2. ModelPort 구현에서 route와 논리 옵션을 검사하고 승인된 키만 provider body로 매핑한다. 임의 옵션 map을 raw body에 merge하지 않는다.

3. SSE parser와 response collector를 작성한다. 잘린 frame·interleaved call·reasoning replay·최종 metadata를 각각 처리하고 전체 한도를 지킨다.

4. inspection은 실제 관찰 또는 신뢰된 snapshot 근거를 사용한다. HTTP redirect와 오류 경로가 credential·raw diagnostic을 누출하지 않는지 검사한다.

## 실제 코드 읽기

`crates/wickle-model-openai/src/model.rs`의 이 단계 11–42행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/25-openai.md)에 있다.

```rust
pub struct OpenAiModel {
    connection: OpenAiConnection,
}
impl OpenAiModel {
    /// Bind an already configured connection without making a network request.
    pub fn new(connection: OpenAiConnection) -> Self {
        Self { connection }
    }
}
impl ModelPort for OpenAiModel {
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
            decoder: response::Decoder::new(request, None),
            framing: sse::Decoder::new(
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

Adapter와 Anti-Corruption Layer의 사례다. 제공자 자료형이 core로 퍼지지 않게 하고 provider-native tool execution을 core tool loop와 혼동하지 않는다. 변환층은 vendor 변경을 격리하지만 모든 새 기능의 매핑·검사가 필요하다. 이 장에서는 아직 OpenAI crate 안의 codec이 다음 장에서 공유 Responses crate로 이동하는 과정을 배운다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-openai --test responses --locked
python3 "$COURSE/lab.py" check 25 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-openai/tests/responses.rs` → `streaming_versions_options_and_reported_usage_do_not_leak_host_context`
- `crates/wickle-model-openai/tests/responses.rs` → `structured_output_preserves_schema_and_rejects_unsupported_contracts_before_http`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs` → `a_replacement_process_recovers_an_interrupted_model_with_a_new_charged_attempt`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs` → `recovery_worker`

### 결함을 주입하는 연습

한 SSE event를 여러 TCP chunk로 나누고 두 tool call의 delta를 교차시켜라. 알 수 없는 옵션을 넣었을 때 fixture 서버가 받은 요청 수를 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

TCP chunk와 SSE event 경계는 같지 않으므로 버퍼링해야 한다. 각 call의 인자는 섞이지 않아야 한다. 잘못된 옵션은 요청 전 거절되어 HTTP 수가 0이어야 한다. hidden SDK retry가 있으면 예산과 호출 수 검사가 깨진다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 25 --dest ../wickle-answer-25
python3 "$COURSE/lab.py" compare 25 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/25-openai.patch"
git apply "$COURSE/solutions/25-openai.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `fb3624adcc52fccd6cb57128cdbfa6a7806f1761`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

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
- `crates/wickle-model-openai/Cargo.toml`
- `crates/wickle-model-openai/src/codec.rs`
- `crates/wickle-model-openai/src/connection.rs`
- `crates/wickle-model-openai/src/inspection.rs`
- `crates/wickle-model-openai/src/lib.rs`
- `crates/wickle-model-openai/src/model.rs`
- `crates/wickle-model-openai/src/response.rs`
- `crates/wickle-model-openai/src/sse.rs`
- `crates/wickle-model-openai/tests/responses.rs`
- `crates/wickle-model-openai/tests/support/mod.rs`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs`
- `crates/wickle-state-sqlite/tests/support/context_recovery.rs`
- `docs/env/openai.md`
- `docs/openai.md`
- `scripts/check-package.py`
- `tests/support/openai_consumer.rs`

</details>
