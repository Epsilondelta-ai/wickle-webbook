# 35장. 독립 Host 통합과 저장소 캐시 검증

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 기본 debug에서의 nested Future·공정성 회귀와 source archive 전체 소비자를 검사한다. 선택 consumer만으로 다른 include 의존자의 빌드를 보장하지 않는다.

이어지는 구현: [58장](58-recovery-audit.md) · [59장](59-host-migration.md).

[목차](README.md) · [이전 장](34-evidence.md) · [다음 장](36-release.md)

## 이번 장의 출발점과 결과

34장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **독립 Host 통합과 저장소 캐시 검증**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/35-integration.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/35-integration.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch13-04-performance.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

workspace 내부 테스트가 성공해도 package에서 빠진 파일이나 dev dependency에 기대는 코드가 있으면 실제 사용자가 빌드하지 못한다. 독립 Host 검사는 라이브러리 archive를 추출한 외부 디렉터리에서 동작한다. 또 SQLite의 동일 checkpoint를 반복 검증하는 비용을 제한된 캐시로 줄인다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 35
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. check-package.py가 만든 .crate만 사용하는 외부 consumer를 실행하고 workspace path 의존이 남지 않는지 검사한다.

2. 서로 독립된 업무 workspace의 policy·input binding·resume·recovery를 끝까지 연결한다.

3. SQLite cache는 같은 scope/DB identity/JSON bytes/checksum일 때만 검증 결과를 재사용한다. 매 operation마다 실제 DB row는 여전히 읽는다.

4. 성공 commit 뒤에만 cache를 공개하고 다른 connection의 갱신·rollback·oversized checkpoint를 검사한다. 32 MiB 제한은 직렬화 JSON 크기이며 decoded graph 메모리는 추가다.

## 실제 코드 읽기

`tests/support/report_process_consumer.rs`의 이 단계 1–32행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/35-integration.md)에 있다.

```rust
// Independent result-generation Host: real subprocesses, SQLite and file output.
#[allow(dead_code)]
mod host {
    include!("adapter_consumer.rs");
    use tokio::io::AsyncWriteExt;
    fn io_error(_: std::io::Error) -> ContractError {
        ContractError::new(ErrorCode::ComponentUnavailable, "report.file")
    }
    async fn trace(directory: &std::path::Path, value: Value) -> Result<(), ContractError> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("trace.jsonl"))
            .await
            .map_err(io_error)?;
        file.write_all(format!("{value}\n").as_bytes())
            .await
            .map_err(io_error)?;
        file.sync_all().await.map_err(io_error)
    }
    struct FileFactory {
        inner: Arc<Factory>,
        directory: std::path::PathBuf,
    }
    impl AdapterFactory for FileFactory {
        fn open<'a>(
            &'a self,
            context: &'a AdapterInitContext,
        ) -> PortFuture<'a, Arc<dyn AdapterInstance>> {
            Box::pin(async move {
                let inner = self.inner.open(context).await?;
                trace(&self.directory,json!({"kind":"open","pid":std::process::id(),"binding":context.execution.binding_set_id})).await?;
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

캐시는 성능 최적화이며 일관성 모델을 바꾸는 기능이 아니다. mutable transaction 객체를 캐시하면 rollback 이전 상태가 유출될 수 있어 검증된 immutable checkpoint만 저장한다. 전체 scope snapshot 구조의 근본적인 성장 비용은 남는다. 독립 consumer는 Component/Contract testing으로 공개 경계를 증명하며 unit test를 대체하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-state-sqlite --test state_store --locked
python3 "$COURSE/lab.py" check 35 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-state-sqlite/tests/state_store.rs` → `durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen`
- `crates/wickle-state-sqlite/tests/state_store.rs` → `failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit`
- `crates/wickle/tests/agent_runtime.rs` → `starting_without_a_tokio_runtime_returns_a_typed_error_before_callbacks`
- `crates/wickle/tests/agent_runtime.rs` → `dropping_a_polled_start_future_does_not_abort_its_owned_admission_or_driver`

### 결함을 주입하는 연습

두 DB 연결에서 번갈아 commit한 뒤 첫 연결이 이전 cache를 돌려주는지 검사하라. 실패한 트랜잭션 직후 read도 확인하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

실제 row가 바뀌면 재검증되어야 한다. 실패한 변경은 cache에도 나타나면 안 된다. 단순히 cache hit 횟수를 검사하는 것보다 오래된 상태를 반환하지 않는 행동을 검사하는 것이 중요하다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 35 --dest ../wickle-answer-35
python3 "$COURSE/lab.py" compare 35 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/35-integration.patch"
git apply "$COURSE/solutions/35-integration.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `c23d50321aac015262639a13abe4537d99e8fd03`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-state-sqlite/src/lib.rs`
- `crates/wickle-state-sqlite/tests/state_store.rs`
- `crates/wickle/tests/agent_runtime.rs`
- `docs/integration-validation.md`
- `docs/sqlite-state-store.md`
- `scripts/check-package.py`
- `tests/support/gather_consumer.rs`
- `tests/support/lifecycle_consumer.rs`
- `tests/support/report_process_consumer.rs`

</details>
