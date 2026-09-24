# 06장. 원자적 저장소와 실행 소유권

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** StateStore는 필수 ExecutionTransactions를 같은 원자 저장 경계에서 구현한다. command 소비·새 segment·lease를 서로 다른 write로 나누면 안 된다.

이어지는 구현: [39장](39-execution-contracts.md) · [40장](40-atomic-store.md) · [48장](48-controls.md).

[목차](README.md) · [이전 장](05-policy.md) · [다음 장](07-budget.md)

## 이번 장의 출발점과 결과

05장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **원자적 저장소와 실행 소유권**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/06-state.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/06-state.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch16-03-shared-state.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

StateStore는 현재 상태뿐 아니라 메시지·보호 레코드·이벤트를 한 변경으로 확정한다. CAS(compare-and-swap)는 기대 revision이 여전히 맞을 때만 다음 revision을 저장한다. lease는 한동안 실행을 소유할 권리이고 fencing generation은 소유자가 바뀐 뒤 예전 worker의 쓰기를 거부하는 증가 번호다. CAS와 lease는 서로 대체되지 않는다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 06
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. StateStore trait과 AdmissionInput/CommitInput을 먼저 작성한다. 조회와 원자적 변경의 반환값을 구분한다.

2. MemoryStateStore에서 scope별 상태를 Mutex로 보호한다. 잠금을 잡은 동안 외부 callback이나 await를 실행하지 않는다.

3. (scope, session_id, request_id)를 중복 키로 사용한다. digest까지 같으면 원래 Run을 반환하고 다르면 RequestConflict로 거절한다.

4. commit은 lease·revision·후보 snapshot·참조·메시지·이벤트를 모두 검증한 뒤 한 번에 적용한다. 검증 중간에 실제 자료구조를 고치지 않는다. terminal commit은 active session 슬롯 해제까지 묶는다.

## 실제 코드 읽기

`crates/wickle/src/state.rs`의 이 단계 21–52행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/06-state.md)에 있다.

```rust
pub struct StateStoreCapabilities {
    /// Records survive process termination.
    pub durable: bool,
    /// Execution leases coordinate independent processes.
    pub cross_process_leases: bool,
    /// Committed events can be replayed in sequence order.
    pub event_replay: bool,
}

/// Immutable, scope-owned data stored with its referencing state and events.
/// Access requires Host authorization; Debug never prints the payload.
#[derive(Clone, PartialEq)]
pub struct ProtectedRecord {
    reference: RecordRef,
    value: Value,
}

impl ProtectedRecord {
    /// Compute the reference digest from owned data. A revision is immutable.
    pub fn new(record_id: Id, revision: u64, value: Value) -> Self {
        Self {
            reference: RecordRef {
                record_id,
                revision,
                digest: canonical_digest(&value),
            },
            value,
        }
    }

    /// Exact immutable record identity, without its payload.
    pub fn reference(&self) -> &RecordRef {
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Repository와 Unit of Work에 가까운 경계다. 다만 범용 CRUD 저장소가 아니라 엔진의 불변 조건을 이해하는 StateStore다. 트랜잭션을 지나치게 작은 CRUD로 쪼개면 일관성 책임이 모든 호출자에게 흩어진다. 반대로 지금 방식은 구현자가 상당히 큰 계약을 구현해야 한다. 이벤트와 snapshot을 같이 저장하지만 상태를 오직 이벤트 재생으로 만드는 순수 Event Sourcing은 아니다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test state --locked
python3 "$COURSE/lab.py" check 06 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/state.rs` → `identical_retries_return_the_original_run_without_replacing_resolved_metadata`
- `crates/wickle/tests/state.rs` → `concurrent_duplicate_admission_creates_exactly_one_run`

### 결함을 주입하는 연습

두 writer가 revision 4를 읽고 각각 revision 5를 commit하게 하라. 첫 commit의 메시지 수와 두 번째 실패 후의 메시지 수를 비교하라. lease 만료 시각과 now가 같을 때도 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

하나만 성공해야 하며 실패한 commit의 메시지나 이벤트가 남으면 안 된다. now == expires_at_ms이면 이미 만료다. 새로운 generation을 받은 worker가 존재하면 예전 owner가 늦게 돌아와도 저장할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 06 --dest ../wickle-answer-06
python3 "$COURSE/lab.py" compare 06 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/06-state.patch"
git apply "$COURSE/solutions/06-state.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `0c4b694b4f8527ff24adafc80097829ed9699b4b`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/tests/state.rs`
- `docs/state.md`
- `tests/support/state_consumer.rs`

</details>
