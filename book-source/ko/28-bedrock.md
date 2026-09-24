# 28장. Bedrock 인증·바이너리 이벤트 프레임

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** HTTP 424 전체가 아니라 명시 ModelStreamErrorException만 Transport로 분류한다. binary frame과 HTTP 경로의 core-owned retry를 함께 검사한다.

이어지는 구현: [53장](53-bedrock-repair.md).

[목차](README.md) · [이전 장](27-anthropic.md) · [다음 장](29-gemini.md)

## 이번 장의 출발점과 결과

27장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Bedrock 인증·바이너리 이벤트 프레임**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/28-bedrock.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/28-bedrock.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch04-03-slices.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Bedrock 경로에는 Messages 형태와 AWS binary event stream이 있다. byte slice를 길이·프레임·checksum 경계에 맞게 읽어야 한다. 문자열 split만으로 binary protocol을 처리할 수 없다. 인증과 모델 target도 Runtime/Mantle 및 operation에 맞아야 한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 28
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 명시적 operation별 endpoint와 인증 계약을 모델링한다. Host가 제공한 credential 만료와 scope를 요청 전에 검사한다.

2. framing parser에서 선언된 길이를 검증한 뒤 제한된 메모리를 할당하고 CRC·header·payload를 검사한다.

3. binary payload의 모델 event를 Anthropic 계열 의미와 연결하되 AWS exception frame은 별도 오류로 처리한다.

4. foundation model과 inference profile의 metadata·destination·revision을 검사한다. origin region과 실제 inference residency를 동일시하지 않는다.

## 실제 코드 읽기

`crates/wickle-model-bedrock/src/framing.rs`의 이 단계 13–44행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/28-bedrock.md)에 있다.

```rust
impl Framing {
    pub fn new(options: &BedrockOptions) -> Self {
        match options.operation {
            BedrockOperation::Messages => Self::Sse(SseDecoder::new(
                options.max_transport_bytes,
                options.max_event_bytes,
                options.max_protocol_events,
            )),
            BedrockOperation::InvokeStream => Self::Aws(AwsFrames {
                buffer: BytesMut::new(),
                bytes: 0,
                frames: 0,
                max_bytes: options.max_transport_bytes,
                max_frame: options.max_event_bytes,
                max_frames: options.max_protocol_events,
            }),
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ContractError> {
        match self {
            Self::Sse(value) => value.push(bytes),
            Self::Aws(value) => value.push(bytes),
        }
    }
    pub fn finish(&self) -> Result<(), ContractError> {
        match self {
            Self::Sse(value) => value.finish(),
            Self::Aws(value) => {
                if value.buffer.is_empty() {
                    Ok(())
                } else {
                    Err(invalid())
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

하위 framing과 상위 모델 의미를 나누는 계층화다. 같은 Claude 계열이라고 AWS transport를 직접 Anthropic HTTP로 취급하지 않는다. codec 재사용은 중복을 줄이지만 재사용 가능한 부분과 서비스별 경계를 구분해야 한다. byte 한도 검사 전에 길이만 믿고 할당하면 공격적 입력으로 메모리를 소진할 수 있다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-bedrock --test bedrock --locked
python3 "$COURSE/lab.py" check 28 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-bedrock/tests/bedrock.rs` → `each_wire_operation_preserves_model_and_uses_its_own_auth_contract`
- `crates/wickle-model-bedrock/tests/bedrock.rs` → `corrupted_truncated_or_oversized_aws_frames_never_complete`

### 결함을 주입하는 연습

정상 frame의 CRC를 한 byte 바꾸고, 길이만 큰 잘린 frame도 보내라. 잘못된 credential로 HTTP가 발생하는지 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

변조·잘림·상한 초과는 완료 ModelResponse로 변환되면 안 된다. credential 오류는 전송 전에 잡혀야 한다. frame의 모델 이름만으로 계정의 실제 사용 권한까지 증명하지 못한다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 28 --dest ../wickle-answer-28
python3 "$COURSE/lab.py" compare 28 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/28-bedrock.patch"
git apply "$COURSE/solutions/28-bedrock.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `7ec497148a959f4f1391c96e69edde4b695938df`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-anthropic/src/codec.rs`
- `crates/wickle-model-anthropic/src/lib.rs`
- `crates/wickle-model-anthropic/src/response.rs`
- `crates/wickle-model-bedrock/Cargo.toml`
- `crates/wickle-model-bedrock/src/auth.rs`
- `crates/wickle-model-bedrock/src/connection.rs`
- `crates/wickle-model-bedrock/src/framing.rs`
- `crates/wickle-model-bedrock/src/inspection.rs`
- `crates/wickle-model-bedrock/src/lib.rs`
- `crates/wickle-model-bedrock/src/model.rs`
- `crates/wickle-model-bedrock/tests/bedrock.rs`
- `crates/wickle-model-bedrock/tests/support/mod.rs`
- `docs/bedrock.md`
- `docs/env/bedrock.md`
- `scripts/check-package.py`
- `tests/support/bedrock_consumer.rs`

</details>
