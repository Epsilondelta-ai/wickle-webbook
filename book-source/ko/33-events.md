# 33장. 영속 이벤트와 외부 메모리 갱신

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 외부 delivery와 Run control command는 다른 원장이다. durable command 저장만으로 remote worker에 알림이 전달되는 것은 아니다.

이어지는 구현: [48장](48-controls.md).

[목차](README.md) · [이전 장](32-mcp.md) · [다음 장](34-evidence.md)

## 이번 장의 출발점과 결과

32장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **영속 이벤트와 외부 메모리 갱신**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/33-events.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/33-events.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch11-03-test-organization.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Run 완료 뒤 메모리 시스템을 갱신하려면 업무 실행과 별도의 delivery journal이 필요하다. core의 durable event는 관찰할 사실을 기록하고 Host가 전달과 재시도를 책임진다. discovery cursor를 앞으로 옮기는 것과 외부 메모리에 실제 반영되는 것은 다른 단계다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 33
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. scope/run/event/subscription revision을 묶은 안정된 delivery ID를 만든다.

2. 한 Host 트랜잭션에서 delivery record와 filter 결정을 저장하고 discovery cursor를 갱신한다. pending delivery는 cursor가 움직여도 남는다.

3. claim generation으로 worker를 구별하고 Applied/Accepted/Unknown/실패 receipt를 별도 상태로 저장한다.

4. run.finished의 보호 outcome은 ReadRecord 권한을 따로 확인한다. 새 Run의 ContextSource로 실제 적용된 메모리만 읽고 source gap은 명시적으로 처리한다.

## 실제 코드 읽기

`tests/host_contract/delivery.rs`의 이 단계 15–46행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/33-events.md)에 있다.

```rust
pub struct Subscription {
    pub scope: Scope,
    pub id: Id,
    pub revision: Id,
    pub target_revision: Id,
}
impl Subscription {
    fn key(&self, run: &Id) -> String {
        canonical_digest(&json!({"scope":self.scope,"run":run,"subscription":self.id,"revision":self.revision,"target":self.target_revision})).as_str().into()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Receipt {
    Applied,
    Accepted(String),
    NotAppliedRetryable,
    PermanentFailure,
    Unknown,
}
#[derive(Debug)]
pub struct Claim {
    pub delivery_id: String,
    pub event: RunEvent,
    pub generation: i64,
    pub operation: Option<String>,
    key: String,
}
/// SQLite belongs to this example Host, independently of the source StateStore.
pub struct Journal {
    connection: Connection,
    subscription: Subscription,
    run: Id,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Outbox와 유사하게 상태와 durable event를 함께 commit하지만 operational queue를 core 안에 넣지는 않는다. 읽기 discovery와 쓰기 application을 나누는 이유는 crash 경계를 드러내기 위해서다. 적어도 한 번 전달과 멱등 소비 조합이지 전 세계 exactly-once 약속이 아니다. 제공 delivery.rs는 계약을 보이는 참조 Host이며 운영 scheduler·retention·backfill은 포함하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-state-sqlite --test host_contract --locked
python3 "$COURSE/lab.py" check 33 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `tests/host_contract/delivery.rs` → `discovery_transaction_survives_restart_and_rolls_back_cursor_failure`
- `tests/host_contract/delivery.rs` → `replaying_older_pages_after_progress_does_not_create_a_gap`

### 결함을 주입하는 연습

외부 서비스가 Accepted를 반환한 뒤 Host를 재시작하라. cursor를 replay하고 새 Run의 메모리를 읽어라. 나중에 Applied로 확인한 뒤 다시 읽어라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

Accepted만으로 메모리 적용을 주장하면 안 된다. 동일 이벤트 재발견은 중복 전달을 늘리지 않아야 하고 Applied 후의 새 Run만 갱신된 자료를 본다. 자체 memory 분석 Run이 다시 이벤트를 생성하는 무한 feedback도 Host가 필터링해야 한다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 33 --dest ../wickle-answer-33
python3 "$COURSE/lab.py" compare 33 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/33-events.patch"
git apply "$COURSE/solutions/33-events.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `a92a8309c4df12999fc4065744749ee79fdd7c69`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-state-sqlite/tests/host_contract.rs`
- `docs/context-sources.md`
- `docs/event-consumers.md`
- `scripts/check-package.py`
- `tests/host_contract/delivery.rs`
- `tests/support/event_consumer.rs`

</details>
