# 53장. Bedrock framing과 재시도 경계

[목차](README.md) · [이전](52-anthropic-repair.md) · [다음](54-gemini-schema.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

52장의 검사를 마친 동일 실습 workspace에서 이어간다. HTTP 424가 항상 재시도 가능한 stream 오류인 것은 아니다. 모든 424를 Transport로 바꾸면 인증·모델 오류까지 코어가 반복 전송할 수 있다.

이번 장의 정확한 checkpoint는 `103d361c7de3e902f434e8a450029f7b39bb3105`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/53-bedrock-repair.md)와 [정답 patch](solutions/53-bedrock-repair.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. 명시 ModelStreamErrorException인 424만 Transport로 분류한다. header와 제한된 body의 code/__type을 정규화한다.

2. Runtime/Mantle Messages와 InvokeStream의 오류 분류를 일치시키고 CRC·frame·취소·deadline 검사를 유지한다.

3. adapter 자체 retry는 계속 금지하고 코어가 첫 transport 실패만 예약 후 재시도하게 한다.

4. 로컬 실제 HTTP로 retry와 4회 인자 repair 뒤 Tool 1회, 총 POST와 model reservations의 일치를 확인한다.

먼저 `python3 "$COURSE/lab.py" inspect 53`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-bedrock/src/framing.rs`의 checkpoint 13행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

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
                }
            }
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Adapter는 오류 의미도 정규화하는 경계다. 너무 넓은 재시도 분류는 비용·효과 위험을 늘린다. named header가 있으면 불필요하게 body를 기다리지 않는 등 제한 시간도 의미의 일부다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-model-bedrock --test bedrock --locked
cargo test -p wickle-model-bedrock --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 53 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-model-bedrock/tests/bedrock.rs::two_documented_releases_keep_independent_selectors`
- `crates/wickle-model-bedrock/tests/bedrock.rs::named_http_stream_errors_are_recoverable_without_classifying_every_424_as_transient`
- `crates/wickle-model-bedrock/tests/bedrock.rs::error_classification_does_not_wait_for_named_bodies_and_bounds_unknown_bodies`

### 예측 → 결함 → 복구

같은 424 status에 알려진 stream exception과 unknown body를 각각 반환하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

둘의 failure kind와 재시도 여부가 달라야 한다. 지역·credential·API 경계 검사는 그대로다. Bedrock live 검증은 release 기록상 별도이며 로컬 통과를 실제 계정 접근 성공으로 말하지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 53 --dest ../wickle-answer-53
python3 "$COURSE/lab.py" compare 53 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/53-bedrock-repair.patch"
git apply "$COURSE/solutions/53-bedrock-repair.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/bedrock.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
