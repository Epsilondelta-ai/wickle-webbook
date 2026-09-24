# 49장. 저장 ID로 조회하는 부작용 없는 진단

[목차](README.md) · [이전](48-controls.md) · [다음](50-openai-schema.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

48장의 검사를 마친 동일 실습 workspace에서 이어간다. 진단 화면을 열 때 모델·source를 다시 실행하면 과거 입력을 진단하는 것이 아니라 오늘의 자료로 새 실행을 하는 것이다. 비용·권한·업무 효과까지 발생할 수 있다.

이번 장의 정확한 checkpoint는 `c2af068d489db44aa5fddaf628b1f99c1f2ed4e0`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/49-inspection.md)와 [정답 patch](solutions/49-inspection.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. inspect_step을 run_id와 StepRef로 제공하고 현재 InspectStep 권한 아래 저장 기록만 읽는다. 순수 converter에는 I/O Port를 주지 않는다.

2. Prepared/DispatchReserved/ResponseObserved/TransmissionUnknown을 근거에 따라 표시한다. 단순 local failure record가 response 수신 증거인지 구분한다.

3. 기본 공개 view에서 system inputs·execution args·connections·opaque·content·민감 option과 schema annotations를 가린다. 원 digest는 재계산하지 않는다.

4. raw context opt-in은 항목별 권한과 최종 전체 fragment 집합의 현재 ACL 검사를 모두 수행한다. 누락·만료·미기록은 구조화해 남긴다.

먼저 `python3 "$COURSE/lab.py" inspect 49`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle/src/agent/inspection.rs`의 checkpoint 5행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
impl Agent {
    /// Inspect saved composition under current permission. This performs only
    /// policy checks and storage reads; it never prepares or executes a step.
    pub async fn inspect_step(
        &self,
        run_id: &Id,
        step: StepRef,
        context: &ExecutionContext,
        options: InspectionOptions,
    ) -> Result<Guarded<CompositionReport>, ContractError> {
        self.check_scope(context)?;
        let timeout = Duration::from_millis(self.inner.bindings.settings.start_timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InspectStep {
                step: step.clone(),
                options,
                context_fragments: vec![],
            },
        };
        if let Guarded::ApprovalRequired(challenge) = self
            .inner
            .bindings
            .policy
            .guard(&request, context, Some(deadline), None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let (report, context_fragments) = caller_read(
            context,
            Some(timeout),
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Query/Command 분리와 pure functional core다. 재실행 없는 진단은 비용과 상태 변경을 막지만 저장하지 않은 이유·estimator identity는 알아낼 수 없다. 설명을 풍부하게 만들려고 오늘의 계산으로 과거 근거를 채우지 않는다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test agent_inspection --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 49 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/agent_inspection.rs::local_empty_stream_or_transport_failure_is_not_proof_of_a_provider_response`
- `crates/wickle/tests/agent_inspection.rs::raw_fragment_set_is_reauthorized_after_later_fragment_checks_revoke_earlier_access`
- `crates/wickle/tests/agent_inspection.rs::a_step_must_belong_to_the_requested_run_and_scope_and_opaque_data_is_never_disclosed`

### 예측 → 결함 → 복구

두 fragment 중 첫 번째를 허용한 뒤 두 번째 조회 도중 첫 번째 권한을 철회하라. 최종 report에 첫 내용이 나오는가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

최종 InspectStep이 포함할 전체 집합을 한 ACL view로 재확인하므로 이전 허용만으로 노출하지 않는다. raw opt-in도 system 실행 인자나 secret 공개를 허용하지 않는다. content는 전체 64KiB 한도이고 잘라 부분 성공으로 만들지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 49 --dest ../wickle-answer-49
python3 "$COURSE/lab.py" compare 49 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/49-inspection.patch"
git apply "$COURSE/solutions/49-inspection.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/step-inspection.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
