# 07장. 예산 예약·시간·취소

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** transport retry/fallback, ToolRepair, 품질 보완을 구분한다. ToolRepair는 잘못된 response round당 한 번 예약하고 다음 모델 호출은 model budget을 따로 쓴다.

이어지는 구현: [42장](42-options.md) · [44장](44-argument-repair.md) · [46장](46-prepared-step.md).

[목차](README.md) · [이전 장](06-state.md) · [다음 장](08-model.md)

## 이번 장의 출발점과 결과

06장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **예산 예약·시간·취소**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/07-budget.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/07-budget.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

모델 응답 한 번을 만드는 논리 step과 실제 HTTP 요청 한 번인 physical attempt는 다르다. 실패 후 재시도하면 같은 step에 attempt가 추가된다. 예산은 성공 횟수가 아니라 시도 허가를 먼저 저장하여 계산한다. 성공할 때만 세면 통신 실패가 반복되는 동안 비용이 무한히 발생할 수 있다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 07
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. Clock과 IdSource를 Host 주입 trait으로 작성한다. 테스트에서는 가짜 시계와 고정 ID를 사용한다.

2. RunTiming에서 실행 중 경과는 monotonic time으로 계산하고 재부착 시에는 저장된 UTC 기준으로 중단 시간도 반영한다.

3. RunBudget의 예약을 저장소 commit으로 만들고 이후에만 외부 작업 closure를 생성한다. Model·Tool·Repair·Recovery 카운터를 구분한다.

4. 예약 뒤 취소되거나 응답을 잃어도 자동 환불하지 않는다. 마지막 예산 슬롯에 대한 경쟁과 이미 사용한 attempt ID의 재사용을 막는다.

## 실제 코드 읽기

`crates/wickle/src/budget.rs`의 이 단계 19–50행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/07-budget.md)에 있다.

```rust
pub struct RunTiming {
    /// Original admission UTC milliseconds; immutable across waiting and resume.
    pub started_at_ms: i64,
    /// Original finite UTC deadline; immutable across waiting and resume.
    pub deadline_at_ms: i64,
    /// Admission time plus saved elapsed time, preserving the monotonic high-water mark.
    pub last_observed_at_ms: i64,
}

impl RunTiming {
    /// Establish a finite deadline at admission without reading a clock implicitly.
    pub fn new(started_at_ms: i64, max_elapsed_ms: u64) -> Result<Self, ContractError> {
        let duration = i64::try_from(max_elapsed_ms)
            .ok()
            .filter(|ms| *ms > 0)
            .ok_or_else(|| failure(ErrorCode::InvalidContract, "timing.duration"))?;
        let deadline_at_ms = started_at_ms
            .checked_add(duration)
            .ok_or_else(|| failure(ErrorCode::InvalidContract, "timing.deadline"))?;
        Ok(Self {
            started_at_ms,
            deadline_at_ms,
            last_observed_at_ms: started_at_ms,
        })
    }
}

/// The counter charged by one saved reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReservationKind {
    /// One physical model call. All purposes share the model-call limit.
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Reservation은 외부 호출 전의 write-ahead 기록에 해당한다. 자원 사용을 보수적으로 계산해 한도를 지키는 대신 실제로 dispatch하지 못한 시도도 과금 카운터에 남을 수 있다. 이는 공급자의 금전 청구액 자체가 아니라 엔진의 실행 한도다. Clock 주입은 테스트 가능성을 높이지만 실제 runtime 시간과 가짜 시계를 혼용하면 테스트가 영원히 기다릴 수 있다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test budget --locked
python3 "$COURSE/lab.py" check 07 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/budget.rs` → `model_purposes_share_a_limit_and_do_not_consume_the_tool_budget`
- `crates/wickle/tests/budget.rs` → `the_last_model_slot_can_only_authorize_one_competing_dispatch`
- `crates/wickle/tests/contracts.rs` → `digest_matches_independent_sha256_vectors_and_sorts_nested_objects`
- `crates/wickle/tests/contracts.rs` → `ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized`

### 결함을 주입하는 연습

max_model_calls=1에서 두 작업을 동시에 예약하라. 예약 저장은 성공했지만 바로 다음에 취소된 경우 카운터가 0으로 되돌아가는지 확인하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

경쟁한 예약 중 하나만 성공한다. 취소된 예약도 저장되어 있으면 1로 남아야 한다. 복구·검증·압축용 모델 호출도 같은 모델 예산을 사용해야 숨은 무제한 호출을 막을 수 있다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 07 --dest ../wickle-answer-07
python3 "$COURSE/lab.py" compare 07 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/07-budget.patch"
git apply "$COURSE/solutions/07-budget.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `5593267ddb790aa98288a277f397b2b457094b78`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `Cargo.toml`
- `crates/wickle/Cargo.toml`
- `crates/wickle/src/budget.rs`
- `crates/wickle/src/clock.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/tests/budget.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/mod.rs`
- `scripts/check-package.py`
- `tests/support/budget_consumer.rs`
- `tests/support/state_consumer.rs`

</details>
