# 24장. 프로세스 장애와 불확실한 효과 복구

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** Interrupted와 legacy active drain, 저장 preparation 복구, command acceptance·새 segment·lease 원자 전이를 함께 다룬다. 구 기록에 없던 actor·boundary를 만들어 재개하지 않는다.

이어지는 구현: [40장](40-atomic-store.md) · [46장](46-prepared-step.md) · [47장](47-interruptions.md) · [48장](48-controls.md) · [58장](58-recovery-audit.md).

[목차](README.md) · [이전 장](23-verification.md) · [다음 장](25-openai.md)

## 이번 장의 출발점과 결과

23장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **프로세스 장애와 불확실한 효과 복구**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/24-recovery.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/24-recovery.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

복구는 저장된 Running Run의 실행 소유자를 새 프로세스가 이어받는 일이다. 가장 위험한 지점은 외부 쓰기가 성공했지만 결과 commit 전에 죽는 경우다. 이 상태를 실패로 단정하고 다시 보내면 중복 효과가 발생한다. reconcile은 원래 시도의 상태를 조회하는 행위이며 다시 실행하는 행위가 아니다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 24
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 현재 권한으로 snapshot을 읽고 recovery_record의 fingerprint와 expected revision을 가진 Recover command를 만든다.

2. 기존 lease가 유효하면 빼앗지 않는다. 새 lease에서 source checkpoint 동일성을 확인하고 recovery 예약과 acceptance를 함께 저장한다.

3. 완료 모델 응답은 재사용하고 미완료 physical attempt는 Interrupted로 남긴다. 새 요청을 보낼 때는 새 attempt와 예산이 필요하다.

4. dispatch 이후 결과가 없는 Tool은 Unknown으로 전환한다. 지원하는 reconcile이나 검증된 외부 receipt만으로 확정하고 확인 불가하면 Waiting을 유지한다.

## 실제 코드 읽기

`crates/wickle/src/agent/recovery.rs`의 이 단계 3–34행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/24-recovery.md)에 있다.

```rust
impl Agent {
    pub(super) async fn recover_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        let bindings = &self.inner.bindings;
        let saved = self
            .resume_read(
                &context,
                bindings.state.load(&bindings.scope, &command.run_id),
            )
            .await?;
        if let Guarded::ApprovalRequired(challenge) =
            self.authorize_resume(&command, &context, None).await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        self.resume_inputs(&saved.snapshot, &context).await?;
        if let Some(receipt) = replayed(&saved.snapshot, &command)? {
            return Ok(Guarded::Completed(
                self.handle(command.run_id, receipt.accepted_revision)?,
            ));
        }
        validate_source(&saved.snapshot, &command)?;
        let lease = bindings
            .state
            .acquire_lease(
                &bindings.scope,
                &command.run_id,
                &bindings.ids.next_id()?,
                bindings.clock.now()?.utc_ms,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

멱등 처리와 복구 가능한 상태 기계다. 분산 트랜잭션 없이 원격 효과와 로컬 DB commit을 하나의 원자 동작으로 만들 수 없으므로 불확실성을 데이터로 표현한다. Saga처럼 보일 수 있지만 Wickle이 일반 보상 트랜잭션 엔진을 제공하는 것은 아니다. “실패하면 반대 작업”은 업무마다 성립 여부가 달라 Host가 설계해야 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-state-sqlite --test agent_recovery --locked
cargo test -p wickle --test agent_recovery --locked
python3 "$COURSE/lab.py" check 24 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-adapter-runtime/tests/context_sources.rs` → `one_catalog_source_can_serve_both_triggers_without_duplicate_runtime_registration`
- `crates/wickle-adapter-runtime/tests/context_sources.rs` → `adapter_source_batch_survives_waiting_while_fresh_instances_reauthorize_its_original_items`
- `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs` → `an_exact_tool_subset_opens_only_after_admission_and_lease_then_releases_once`
- `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs` → `missing_admission_wrong_scope_or_lost_lease_prevents_factory_entry`

### 결함을 주입하는 연습

write callback 뒤 결과 저장 직전에 프로세스를 종료한다. 새 프로세스에서 원래 idempotency key를 조회해 이미 Applied임을 확인하라. 조회 불가인 경우도 반복하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

첫 경우 재실행 없이 원래 call을 settle한다. 두 번째는 unknown을 보존하고 후속 실행을 멈춘다. storage 장애 중 반환하는 last_confirmed_revision은 로컬 진단이며 acknowledgement를 잃은 최신 commit보다 오래될 수 있다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 24 --dest ../wickle-answer-24
python3 "$COURSE/lab.py" compare 24 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/24-recovery.patch"
git apply "$COURSE/solutions/24-recovery.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `fdcd93e7ce36e82717d9dfa93939c2122f7e5f05`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-adapter-runtime/src/lifecycle.rs`
- `crates/wickle-adapter-runtime/tests/context_sources.rs`
- `crates/wickle-adapter-runtime/tests/runtime_lifecycle.rs`
- `crates/wickle-adapter-runtime/tests/support/context_sources.rs`
- `crates/wickle-adapter-runtime/tests/support/mod.rs`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs`
- `crates/wickle-state-sqlite/tests/support/context_recovery.rs`
- `crates/wickle-state-sqlite/tests/support/recovery_store.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/persistence.rs`
- `crates/wickle/src/agent/recovery.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/budget.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/recovery.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/state/reconciliation_state.rs`
- `crates/wickle/src/state/recovery_state.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/reconciliation.rs`
- `crates/wickle/src/tool_execution/resume.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/src/views.rs`
- `crates/wickle/tests/agent_recovery.rs`
- `crates/wickle/tests/agent_resume.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/agent_resume.rs`
- `crates/wickle/tests/support/mod.rs`
- `crates/wickle/tests/support/tool_execution.rs`
- `crates/wickle/tests/tool_execution.rs`
- `docs/agents.md`
- `docs/recovery.md`
- `tests/support/budget_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/recovery_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`

</details>
