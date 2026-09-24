# 27장. Anthropic Messages와 재현 가능한 복구 검사

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** Native schema는 유지하고 invalid raw Tool inputs만 새 replay envelope로 보존한다. 정상 v1 continuation·서명은 그대로 유지한다.

이어지는 구현: [52장](52-anthropic-repair.md).

[목차](README.md) · [이전 장](26-azure.md) · [다음 장](28-bedrock.md)

## 이번 장의 출발점과 결과

26장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Anthropic Messages와 재현 가능한 복구 검사**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/27-anthropic.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/27-anthropic.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch11-03-test-organization.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Anthropic Messages는 content block, thinking, tool_use와 tool_result의 구조를 갖는다. thinking이나 서명은 사용자에게 보일 일반 텍스트와 다를 수 있다. 따라서 답변 text만 저장했다가 재전송하면 provider가 요구하는 원래 continuation을 잃을 수 있다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 27
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. Messages codec에서 system instruction과 대화 content를 명시적으로 변환한다.

2. block별 상태와 tool arguments를 모으고 thinking signature와 원래 순서를 opaque continuation에 보존한다.

3. usage가 누적값이면 delta처럼 더하지 않는다. 불완전·서명 누락·순서 충돌을 실패로 분류한다.

4. 이 checkpoint에는 복구 소비자의 timing 보정도 포함된다. 고정 sleep으로 디스크 완료를 추측하지 않고 관찰 가능한 상태를 기준으로 동기화하는 변경을 읽는다.

## 실제 코드 읽기

`crates/wickle-model-anthropic/src/response.rs`의 이 단계 1–32행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/27-anthropic.md)에 있다.

```rust
use crate::{
    codec::{self, invalid, nonempty, string},
    error,
};
use serde_json::{Value, json};
use wickle::*;
use wickle_model_responses::SseEvent;

struct Block {
    value: Value,
    arguments: String,
    closed: bool,
}
pub(crate) struct Decoder<'a> {
    request: &'a ModelRequest,
    pub metadata: ModelResponseMetadata,
    message_id: Option<String>,
    blocks: Vec<Block>,
    stop: Option<String>,
    terminal: Option<ModelEvent>,
    bytes: usize,
    emitted: usize,
    tools: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(request: &'a ModelRequest, request_id: Option<Id>) -> Self {
        Self {
            request,
            metadata: ModelResponseMetadata {
                provider_request_id: request_id,
                ..Default::default()
            },
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

프로토콜 parser 자체도 상태 기계다. 같은 stream을 text concatenation으로 단순화하면 block 구분과 증명 자료가 손실된다. 테스트의 동기화도 설계 문제다. 벽시계 sleep은 빠르지만 CI·느린 디스크에서 flaky하고, 상태 기반 대기는 의도를 드러내는 대신 timeout과 관찰 조건을 정확히 정의해야 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-anthropic --test messages --locked
python3 "$COURSE/lab.py" check 27 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-anthropic/tests/messages.rs` → `thinking_before_text_and_cumulative_usage_are_not_misread_as_visible_output`
- `crates/wickle-model-anthropic/tests/messages.rs` → `signed_empty_thinking_and_tool_inputs_are_replayed_once_with_results`

### 결함을 주입하는 연습

빈 thinking text에 유효 signature가 있는 응답과 서명이 빠진 응답을 구별하라. cumulative usage가 두 번 오면 총량을 단순 합산하는 결함을 넣어 테스트가 잡는지 확인하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

빈 문자열이라는 이유로 필요한 signed block을 버리면 안 된다. 누적 usage를 더하면 사용량이 과대 계산된다. 제공자 미보고 값은 unknown으로 남기며 0이나 추정 모델 버전으로 채우지 않는다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 27 --dest ../wickle-answer-27
python3 "$COURSE/lab.py" compare 27 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/27-anthropic.patch"
git apply "$COURSE/solutions/27-anthropic.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `cadda4c5015f262d428654b58c7afb822c4eb015`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-anthropic/Cargo.toml`
- `crates/wickle-model-anthropic/src/codec.rs`
- `crates/wickle-model-anthropic/src/connection.rs`
- `crates/wickle-model-anthropic/src/inspection.rs`
- `crates/wickle-model-anthropic/src/lib.rs`
- `crates/wickle-model-anthropic/src/model.rs`
- `crates/wickle-model-anthropic/src/response.rs`
- `crates/wickle-model-anthropic/tests/messages.rs`
- `crates/wickle-model-anthropic/tests/support/mod.rs`
- `docs/anthropic.md`
- `docs/env/anthropic.md`
- `docs/env/azure-openai.md`
- `docs/env/bedrock.md`
- `docs/env/gemini.md`
- `docs/env/openai.md`
- `docs/env/vertex-ai.md`
- `docs/env/xai.md`
- `scripts/check-package.py`
- `tests/support/anthropic_consumer.rs`
- `tests/support/recovery_consumer.rs`

</details>
