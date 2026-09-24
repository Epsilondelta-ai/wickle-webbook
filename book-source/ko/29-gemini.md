# 29장. Gemini 스트림·도구 순서·서명

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** provider native schema로 표현 못하는 제약은 core 검증과 문맥으로 보존한다. codec 반환형 Vec<u8>는 HTTP body로 직접 전송한다.

이어지는 구현: [54장](54-gemini-schema.md).

[목차](README.md) · [이전 장](28-bedrock.md) · [다음 장](30-vertex.md)

## 이번 장의 출발점과 결과

28장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Gemini 스트림·도구 순서·서명**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/29-gemini.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/29-gemini.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch08-01-vectors.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Gemini 응답의 parts에는 텍스트, function call, thought signature 등이 함께 올 수 있다. wire call ID가 없는 여러 호출도 원래 순서를 보존해야 올바른 result와 연결된다. 엔진 ID를 생성할 수 있다는 사실과 provider가 실제 ID를 보고했다는 주장은 다르다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 29
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. generateContent request를 모델 route/API version과 맞추고 공개 옵션을 허용 목록으로 변환한다.

2. parts별 호출 순서와 signature를 보존하며 ModelEvent로 정규화한다. 결과 replay도 원래 call 순서와 맞춘다.

3. native schema 표현이 원래 closed-object 제약을 보존하지 못하면 조용히 제약을 버리지 말고 지원 불가를 반환한다.

4. 요청 모델과 관찰된 revision, usage 누적/미보고를 분리한다. refusal·length·truncated stream을 성공으로 바꾸지 않는다.

## 실제 코드 읽기

`crates/wickle-model-gemini/src/response.rs`의 이 단계 11–42행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/29-gemini.md)에 있다.

```rust
pub struct Decoder<'a> {
    request: &'a ModelRequest,
    /// Provider-reported facts only; omitted versions and usage remain unknown.
    pub metadata: ModelResponseMetadata,
    parts: Vec<Value>,
    ids: Vec<String>,
    seen_ids: BTreeSet<String>,
    finish: Option<ModelFinish>,
    bytes: usize,
    started: bool,
}
impl<'a> Decoder<'a> {
    /// Start one physical attempt without network access.
    pub fn new(request: &'a ModelRequest) -> Self {
        Self {
            request,
            metadata: Default::default(),
            parts: vec![],
            ids: vec![],
            seen_ids: BTreeSet::new(),
            finish: None,
            bytes: 0,
            started: false,
        }
    }
    /// Consume a complete SSE JSON record. It is never a Tool execution permit.
    pub fn event(&mut self, event: SseEvent) -> Result<Vec<ModelEvent>, ContractError> {
        if event.name.as_deref().is_some_and(|s| s != "message") {
            return Err(invalid());
        }
        let value = parse_json(&event.data)?;
        if !value.is_object() {
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Adapter는 데이터 모양만 바꾸는 thin wrapper가 아니라 의미 보존 경계다. 모든 제공자를 최저 공통 기능으로 낮추면 이식성은 쉬워지지만 JSON 계약과 continuation 기능을 잃는다. Wickle은 명시적 capability와 제한을 통해 표현 불가를 드러내는 쪽을 선택한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-gemini --test generate --locked
python3 "$COURSE/lab.py" check 29 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-gemini/tests/generate.rs` → `explicit_versions_and_resource_prefix_preserve_wire_and_usage`
- `crates/wickle-model-gemini/tests/generate.rs` → `parallel_calls_replay_signatures_without_inventing_wire_ids`

### 결함을 주입하는 연습

ID 없는 function call 두 개와 각각 다른 signature를 반환하고 결과를 역순으로 연결하는 결함을 넣어라. additionalProperties=false를 변환 과정에서 지워 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

순서·signature replay 검사가 실패해야 한다. schema 제약 삭제는 겉보기 성공이지만 실행 계약 위반이다. byte 수와 token estimate도 다른 단위이므로 하나로 다른 한도를 증명할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 29 --dest ../wickle-answer-29
python3 "$COURSE/lab.py" compare 29 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/29-gemini.patch"
git apply "$COURSE/solutions/29-gemini.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `ec2de798d4e6a6982f4370e31c81eaa82420b28d`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-gemini/Cargo.toml`
- `crates/wickle-model-gemini/src/codec.rs`
- `crates/wickle-model-gemini/src/connection.rs`
- `crates/wickle-model-gemini/src/inspection.rs`
- `crates/wickle-model-gemini/src/lib.rs`
- `crates/wickle-model-gemini/src/model.rs`
- `crates/wickle-model-gemini/src/response.rs`
- `crates/wickle-model-gemini/tests/generate.rs`
- `crates/wickle-model-gemini/tests/support/mod.rs`
- `docs/env/gemini.md`
- `docs/gemini.md`
- `scripts/check-package.py`
- `tests/support/gemini_consumer.rs`

</details>
