# 47장. 중단 정책과 업무 app_state

[목차](README.md) · [이전](46-prepared-step.md) · [다음](48-controls.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

46장의 검사를 마친 동일 실습 workspace에서 이어간다. Host를 정리하려고 멈추는 것과 사용자가 업무를 취소하는 것은 다르다. 앱은 검토 대기·유지보수 같은 자기 상태를 남기고 싶지만 core status의 의미를 바꾸면 안 된다.

이번 장의 정확한 checkpoint는 `17ef0ca0fd31607bdfa1496f19dbd12169aa725c`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/47-interruptions.md)와 [정답 patch](solutions/47-interruptions.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. InterruptionPolicyBinding의 identity/config/schema/timeout을 admission에서 고정한다. callback은 불변 InterruptionInfo만 받고 data proposal을 반환한다.

2. UseDefault/Pause/Cancel/Fail과 optional AppState를 검증한다. AppStateSchema는 namespace/status/metadata 전체에 적용한다.

3. stop_execution의 기본은 가능한 checkpoint의 Interrupted다. callback 오류·panic·timeout·무효 결정은 기본 정책과 보호 진단으로 처리한다.

4. 결정·app_state·outcome을 저장하고 lease를 반납한 뒤 자원을 정리한다. 이미 진입한 write의 Unknown은 보존한다.

먼저 `python3 "$COURSE/lab.py" inspect 47`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/agent/interruption.rs`의 checkpoint 5행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
impl Agent {
    pub(super) async fn interruption_plan(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<(RecordRef, InterruptionPlan), ContractError> {
        let reference = snapshot
            .interruption_plan_ref
            .as_ref()
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "interruption.plan"))?;
        let record = self
            .inner
            .bindings
            .state
            .read_record(&snapshot.scope, reference)
            .await?;
        let plan: InterruptionPlan = serde_json::from_value(record.value().clone())
            .map_err(|_| fail(ErrorCode::InvalidSnapshot, "interruption.plan"))?;
        plan.validate()?;
        Ok((reference.clone(), plan))
    }
    pub(super) async fn validate_interruption_binding(
        &self,
        snapshot: &RunSnapshot,
    ) -> Result<(), ContractError> {
        let (_, saved) = self.interruption_plan(snapshot).await?;
        let current = InterruptionPlan::capture(
            self.inner.bindings.interruption_policy.as_ref(),
            saved.timeout_ms,
        )?;
        if current.policy != saved.policy
            || current.configuration != saved.configuration
            || current.app_state_schema != saved.app_state_schema
            || current.timeout_ms != saved.timeout_ms
        {
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Strategy callback에 상태 전이의 최종 권한을 주지 않는 설계다. 앱은 업무 상태를 확장할 수 있지만 명시 취소·예산 소진·소유권 상실을 뒤집을 수 없다. 기본 callback 1초, cleanup 5초는 협조적 async 상한이지 blocking 코드를 강제 종료하는 sandbox가 아니다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test agent_interruption --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 47 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/agent_interruption.rs::lost_ownership_never_invokes_policy_commits_a_stop_or_releases_the_old_lease`
- `crates/wickle/tests/agent_interruption.rs::restored_stops_require_their_event_exact_checkpoint_and_cause_consistent_outcome`
- `crates/wickle/tests/agent_interruption.rs::a_live_stop_commit_cannot_omit_the_interrupted_event`

### 예측 → 결함 → 복구

HostShutdown에서 operations/maintenance app_state로 Pause를 반환하고, UserCancel에도 같은 Pause를 반환해 본다.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

유효한 HostShutdown은 복구 가능한 Interrupted가 되고 Session은 점유 상태다. 명시 취소는 callback의 Pause로 바뀌지 않는다. app_state는 16KiB, 저장 policy plan은 64KiB 한도를 따르며 credential 저장 용도가 아니다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 47 --dest ../wickle-answer-47
python3 "$COURSE/lab.py" compare 47 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/47-interruptions.patch"
git apply "$COURSE/solutions/47-interruptions.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/interruption-policy.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
