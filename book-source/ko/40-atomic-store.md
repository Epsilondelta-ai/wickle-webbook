# 40장. segment·lease·command를 한 트랜잭션으로 저장하기

[목차](README.md) · [이전](39-execution-contracts.md) · [다음](41-admission.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

39장의 검사를 마친 동일 실습 workspace에서 이어간다. resume command를 먼저 소비하고 새 lease를 나중에 얻는 두 번의 저장으로 구현하면 중간 crash에서 명령만 사라진다. 반대 순서는 명령 하나에 여러 worker가 실행될 수 있다.

이번 장의 정확한 checkpoint는 `5c04c0e6a32a460fb8c71de42c9dd51d94d0ec56`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/40-atomic-store.md)와 [정답 patch](solutions/40-atomic-store.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. StateStore가 ExecutionTransactions를 포함하게 하고 begin_segment를 구현한다. 명령 수락·새 segment·lease generation·checkpoint·events를 한 commit에 묶는다.

2. Memory는 lock 안의 작업 사본을 모두 검증한 뒤 교체한다. SQLite는 BEGIN IMMEDIATE 아래 같은 core 검증을 재사용한다.

3. duplicate acceptance는 기존 segment를 반환하고 새 lease를 지급하지 않는다. command ID의 다른 payload와 segment ID 재사용을 거절한다.

4. legacy v1 읽기는 쓰지 않는다. active legacy scope는 신규 admission을 막고 terminal만 남은 scope의 새 admission에서 v2로 원자 전환한다.

먼저 `python3 "$COURSE/lab.py" inspect 40`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/state/execution.rs`의 checkpoint 272행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
impl ExecutionTransactions for MemoryStateStore {
    fn read_execution<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
    ) -> PortFuture<'a, ExecutionHistory> {
        Box::pin(async move {
            let scopes = self.lock()?;
            let state = namespace(&scopes, scope)?;
            if !state.runs.contains_key(run_id) {
                return Err(not_found());
            }
            state.executions.get(run_id).cloned().ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
                    "execution.legacy_checkpoint",
                )
            })
        })
    }
    fn submit_control_command<'a>(
        &'a self,
        scope: &'a Scope,
        run_id: &'a Id,
        command: ControlCommand,
    ) -> PortFuture<'a, ControlReceipt> {
        Box::pin(async move {
            validate_control(&command)?;
            let mut scopes = self.lock()?;
            let state = scopes.get_mut(&scope_key(scope)).ok_or_else(not_found)?;
            let run = state.runs.get(run_id).ok_or_else(not_found)?;
            let history = state.executions.get_mut(run_id).ok_or_else(|| {
                error(
                    ErrorCode::CapabilityUnsupported,
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Unit of Work를 저장소 인터페이스의 의미로 강제한다. transaction을 여러 CRUD 호출로 에뮬레이션하지 않아 구현 부담은 커지지만 모든 backend가 같은 경쟁 조건을 지킬 수 있다. DB transaction이 외부 결제까지 rollback하는 것은 아니다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test execution_store --locked
cargo test -p wickle-state-sqlite --test execution_store --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 40 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/execution_store.rs::version_two_rejects_missing_active_history_even_when_other_histories_remain`
- `crates/wickle/tests/execution_store.rs::restored_receipts_must_match_the_original_recovery_command_and_segment`
- `crates/wickle/tests/execution_store.rs::version_two_cannot_silently_reclassify_new_terminal_runs_as_legacy`
- `crates/wickle-state-sqlite/tests/execution_store.rs::sqlite_execution_transactions_survive_reopen`
- `crates/wickle-state-sqlite/tests/execution_store.rs::sqlite_recovery_rolls_back_and_replays_the_same_segment`
- `crates/wickle-state-sqlite/tests/execution_store.rs::legacy_terminal_rows_are_read_without_rewrite_then_upgrade_on_new_admission`

### 예측 → 결함 → 복구

동일 command를 barrier로 동시에 보내고 실패한 candidate 뒤에 lease·event·receipt 일부가 남는지 본다.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

하나의 acceptance와 원 segment만 있어야 한다. 실패하면 ownership·command 소비도 함께 rollback한다. v2 기록에서 일부 실행 이력이 사라졌다고 그 Run을 legacy로 재분류하여 통과시키면 안 된다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 40 --dest ../wickle-answer-40
python3 "$COURSE/lab.py" compare 40 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/40-atomic-store.patch"
git apply "$COURSE/solutions/40-atomic-store.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/state.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
