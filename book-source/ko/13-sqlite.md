# 13장. SQLite에 실행 상태 영속화하기

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 읽기는 구 checkpoint를 바꾸지 않지만 지원되는 새 admission은 v2 checkpoint로 원자 upgrade한다. 따라서 과거의 “migration 없음” 설명을 최종 0.2.0에 적용하지 않는다. legacy active는 drain한다.

이어지는 구현: [40장](40-atomic-store.md) · [59장](59-host-migration.md).

[목차](README.md) · [이전 장](12-catalog.md) · [다음 장](14-routing.md)

## 이번 장의 출발점과 결과

12장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **SQLite에 실행 상태 영속화하기**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/13-sqlite.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/13-sqlite.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch16-01-threads.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

메모리 저장소를 다시 만드는 것은 복구가 아니다. SQLite adapter는 파일을 다시 열어 이전 상태·lease generation·이벤트를 읽는다. 각 scope의 전체 checkpoint를 직렬화해서 core의 검증 로직을 재사용한다. 행 단위 도메인 테이블로 완전히 정규화한 설계와는 비용 특성이 다르다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 13
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 별도 wickle-state-sqlite crate를 만들고 StateStore를 구현한다. DB 파일 경로와 busy timeout은 Host가 준다.

2. 쓰기에서 BEGIN IMMEDIATE로 잠금을 얻은 뒤 최신 checkpoint를 읽고 MemoryStateStore의 상태 검증을 적용한다.

3. checkpoint JSON과 checksum을 트랜잭션으로 저장한 뒤에만 성공을 반환한다. 잘못된 버전·scope·참조·checksum을 거절한다.

4. blocking SQLite 작업을 Tokio blocking pool로 옮긴다. await를 취소했다고 이미 실행 중인 DB 작업이 rollback됐다고 가정하지 않는다. 별도 프로세스로 재개방해 검사한다.

## 실제 코드 읽기

`crates/wickle-state-sqlite/src/lib.rs`의 이 단계 55–86행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/13-sqlite.md)에 있다.

```rust
pub struct SqliteStateStore {
    path: PathBuf,
    busy_timeout: Duration,
    store_id: String,
}

impl fmt::Debug for SqliteStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStateStore")
            .field("busy_timeout", &self.busy_timeout)
            .finish_non_exhaustive()
    }
}

impl SqliteStateStore {
    /// Open or initialize a dedicated disk database with a five-second busy timeout.
    ///
    /// This is synchronous initialization; call it before starting asynchronous
    /// work, or place it in the Host's blocking task. No runtime is created.
    /// New files use owner-only permissions on Unix. Existing permissions are
    /// retained. Empty paths, SQLite URI paths and :memory: are rejected.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ContractError> {
        Self::open_with_busy_timeout(path, DEFAULT_BUSY_TIMEOUT)
    }

    /// Open with an explicit bounded SQLite lock wait, from zero to thirty seconds.
    /// Zero requests immediate failure on lock contention. SQLite measures this
    /// setting in milliseconds. Other database and filesystem work is not timed by
    /// this option, and queued/running blocking tasks can outlive their waiter.
    pub fn open_with_busy_timeout(
        path: impl AsRef<Path>,
        busy_timeout: Duration,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

StateStore에 대한 Adapter다. core의 상태 검증을 중복 구현하지 않아 계약 유지가 쉬워지는 대신 scope 기록 전체의 직렬화·복원 비용이 커진다. SQLite의 단일 writer와 로컬 WAL 특성 때문에 큰 다중 서버 SaaS용 저장소와 동일시하면 안 된다. 행 단위 저장은 변경량을 줄일 수 있지만 트랜잭션과 모든 불변 조건을 다시 정확히 구현해야 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-state-sqlite --test state_store --locked
python3 "$COURSE/lab.py" check 13 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-state-sqlite/tests/state_store.rs` → `durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen`
- `crates/wickle-state-sqlite/tests/state_store.rs` → `failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit`
- `crates/wickle/tests/state_checkpoint.rs` → `exporting_one_namespace_excludes_another_scope_with_the_same_identifiers`
- `crates/wickle/tests/state_checkpoint.rs` → `checkpoint_preserves_request_identity_private_records_and_lease_generation`

### 결함을 주입하는 연습

DB 파일을 닫고 새로운 프로세스에서 outcome과 이벤트를 읽어라. 서로 다른 두 연결에서 동일 revision의 변경을 시도하라. 메모리 store의 capability와 비교하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

파일 store는 재시작 후 상태를 보존하고 오래된 writer를 막아야 한다. 메모리 store에 checkpoint를 복원할 수 있어도 durable/cross_process_leases=true라고 선언할 수 없다. 이 테스트는 하드웨어 전원 손실까지 시뮬레이션한 것은 아니다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 13 --dest ../wickle-answer-13
python3 "$COURSE/lab.py" compare 13 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/13-sqlite.patch"
git apply "$COURSE/solutions/13-sqlite.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `757fc97f7ccceadbd697e4f32056a92cdf0f95df`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `Cargo.toml`
- `crates/wickle-state-sqlite/Cargo.toml`
- `crates/wickle-state-sqlite/src/lib.rs`
- `crates/wickle-state-sqlite/tests/state_store.rs`
- `crates/wickle-state-sqlite/tests/support/mod.rs`
- `crates/wickle-state-sqlite/tests/support/workers.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/tests/state_checkpoint.rs`
- `docs/contracts.md`
- `docs/sqlite-state-store.md`
- `scripts/check-package.py`
- `tests/support/sqlite_consumer.rs`

</details>
