# 48장. 원자적 재개·취소·만료와 불변 handle

[목차](README.md) · [이전](47-interruptions.md) · [다음](49-inspection.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

47장의 검사를 마친 동일 실습 workspace에서 이어간다. 이전 대기 handle의 outcome을 나중의 cancel 결과로 바꾸면 그 handle을 관찰한 두 사용자가 서로 다른 사실을 보게 된다. 원 실행자와 재개 승인자도 동일한 신원은 아니다.

이번 장의 정확한 checkpoint는 `725274f0663bd1bfaca99a9100a0724b0dd7719d`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/48-controls.md)와 [정답 patch](solutions/48-controls.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. ControlCommand에 stable command_id·principal_ref·action을 담고 현재 인증과 일치하는지 검사한다. submit 결과는 수락 receipt이며 처리 완료와 구분한다.

2. resume acceptance·새 segment·lease/fencing·상태 전이를 하나로 확정한다. 원 execution principal/grant를 유지하고 reviewer는 승인 근거로 따로 둔다.

3. Waiting/Interrupted의 Cancel·기한 지난 Expire는 control-only segment에서 수행한다. 모델·도구를 호출하지 않고 과거 handle 결과를 보존한다.

4. get_run·get_control_receipt는 읽기만 한다. deadline_expired 관찰과 실제 Expire 명령 처리를 구분한다.

먼저 `python3 "$COURSE/lab.py" inspect 48`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/agent/control.rs`의 checkpoint 3행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
impl Agent {
    /// Persist an authorized, deduplicated control. Receipt is acceptance, not
    /// completion; remote Worker notification remains the Host's responsibility.
    pub async fn submit_control_command(
        &self,
        run_id: Id,
        command: ControlCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<ControlReceipt>, ContractError> {
        self.check_scope(&context)?;
        if command.principal_ref != context.data.principal_ref {
            return Err(fail(ErrorCode::AccessDenied, "control.principal"));
        }
        let agent = self.clone();
        tokio::runtime::Handle::try_current()
            .map_err(|_| fail(ErrorCode::RuntimeUnavailable, "agent.runtime"))?
            .spawn(crate::future::boxed(|| async move {
                agent.submit_control_owned(run_id, command, context).await
            }))
            .await
            .map_err(|_| fail(ErrorCode::InvalidContract, "control.coordinator"))?
    }

    async fn submit_control_owned(
        &self,
        run_id: Id,
        command: ControlCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<ControlReceipt>, ContractError> {
        let bindings = &self.inner.bindings;
        let action = match command.action {
            ControlAction::Cancel { .. } => PolicyAction::CancelRun {},
            ControlAction::Stop { cause } => PolicyAction::StopExecution { cause },
            ControlAction::Expire => PolicyAction::ExpireRun {},
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Command와 segment별 불변 history다. 논리 Run은 이어져도 handle은 한 구간을 가리킨다. durable control은 저장된 의도이며 worker 알림 전달 서비스까지 core가 제공하지 않는다. convenience cancel(reason, context)는 새 ID를 생성하므로 stable retry가 필요하면 명시 command API를 쓴다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test agent_control --locked
cargo test -p wickle --test agent_resume --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 48 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/agent_control.rs::durable_stop_records_custom_pause_cancel_and_fail_decisions`
- `crates/wickle/tests/agent_control.rs::active_expiry_is_pending_before_the_deadline_and_consumed_when_the_budget_ends`
- `crates/wickle/tests/agent_control.rs::denied_worker_processing_and_receipt_reads_leave_the_pending_command_unchanged`
- `crates/wickle/tests/agent_resume.rs::expired_recovery_preserves_an_unconfirmed_write_without_querying_or_dispatching`
- `crates/wickle/tests/agent_resume.rs::persistent_storage_failure_reports_the_last_confirmed_revision_and_uncertain_effect_under_current_permission`
- `crates/wickle/tests/agent_resume.rs::a_concurrent_duplicate_acceptance_cannot_override_a_fresh_permission_denial`

### 예측 → 결함 → 복구

Waiting handle을 보관한 채 다른 Host에서 cancel하고 두 handle/get_run/receipt를 비교하라. 만료 시점에 조회만 반복하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

예전 handle은 Waiting, 최신 Run은 Cancelled, receipt는 처리 segment를 가리킨다. 조회로 revision·usage·lease가 바뀌면 잘못이다. 아직 이른 Expire는 pending이고 기존 active lease를 훔치지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 48 --dest ../wickle-answer-48
python3 "$COURSE/lab.py" compare 48 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/48-controls.patch"
git apply "$COURSE/solutions/48-controls.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/run-controls.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
