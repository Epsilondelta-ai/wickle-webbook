# 17장. 승인·입력·외부 결과를 기다리고 재개

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 과거 “다른 검토자가 current principal을 갱신한다”는 설명은 원래 execution actor 보존 계약으로 바뀐다. idle cancel도 새 control-only segment를 만들므로 옛 Waiting handle outcome은 그대로다.

이어지는 구현: [48장](48-controls.md).

[목차](README.md) · [이전 장](16-tools.md) · [다음 장](18-hooks.md)

## 이번 장의 출발점과 결과

16장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **승인·입력·외부 결과를 기다리고 재개**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/17-resume.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/17-resume.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch06-03-if-let.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Waiting은 worker를 계속 붙들고 있는 sleep이 아니라 저장된 중단 지점이다. ResumeCommand는 run/revision/command ID와 정확한 wait 대상에 대한 결정을 담는다. 승인한 사람이 바뀌어도 기존 system input과 도구 대상은 유지한다. 기다린 시간은 원래 Run deadline에 포함된다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 17
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 승인·입력·외부 확인 wait별로 고정된 target을 저장한다. facade 작업의 Guarded::ApprovalRequired와 Run의 Waiting을 구별한다.

2. resume은 현재 권한, expected revision, wait ID, target, 입력 schema를 검사하고 command 소비와 상태 전이를 원자적으로 commit한다.

3. 동일 command 재전송은 기존 acceptance를 반환한다. 같은 ID로 다른 결정을 보내면 conflict이고 terminal Run에 새 명령을 넣을 수 없다.

4. 새 execution segment와 handle을 만든다. 입력 응답은 저장된 출력 schema로 원래 call을 완료하며 질문 도구를 다시 실행하지 않는다. external receipt는 Host verifier가 확인한다.

## 실제 코드 읽기

`crates/wickle/src/agent/resume.rs`의 이 단계 4–35행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/17-resume.md)에 있다.

```rust
impl Agent {
    pub(super) async fn resume_command(
        &self,
        command: ResumeCommand,
        context: ExecutionContext,
    ) -> Result<Guarded<RunHandle>, ContractError> {
        self.check_scope(&context)?;
        if matches!(
            command.action,
            ResumeAction::Recover { .. }
                | ResumeAction::Approve {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
                | ResumeAction::Deny {
                    target: ApprovalTarget::Candidate { .. },
                    ..
                }
        ) {
            return Err(fail(
                ErrorCode::CapabilityUnsupported,
                "agent.resume_action",
            ));
        }
        if serde_json::to_vec(&command)
            .map_err(|_| fail(ErrorCode::InvalidJson, "agent.command"))?
            .len()
            > self.inner.bindings.settings.max_request_bytes
        {
            return Err(fail(ErrorCode::InvalidContract, "agent.command_size"));
        }
        let saved = self
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

명시적 상태 기계와 멱등 Command 처리다. 논리 Run과 실행 segment를 분리하면 오래 기다리는 작업이 프로세스 재시작 뒤에도 이어진다. 상태 수와 재개 검증은 늘어나지만 버튼 중복 클릭·네트워크 재전송을 정상 시나리오로 다룰 수 있다. approval은 미래의 모든 행동을 허용하는 bearer token이 아니며 현재 정책을 우회하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test agent_resume --locked
python3 "$COURSE/lab.py" check 17 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-state-sqlite/tests/state_store.rs` → `durable_admission_replays_the_original_run_and_releases_a_finished_session_after_reopen`
- `crates/wickle-state-sqlite/tests/state_store.rs` → `failed_transactions_leave_no_records_or_events_and_stale_revisions_cannot_commit`
- `crates/wickle/tests/agent_resume.rs` → `approval_continues_the_same_run_with_frozen_inputs_and_a_distinct_segment_handle`
- `crates/wickle/tests/agent_resume.rs` → `waiting_consumes_no_new_calls_and_late_approval_keeps_its_charged_reservation`

### 결함을 주입하는 연습

승인 명령을 두 번 보내고 두 번째는 네트워크 응답을 잃은 재시도라고 가정하라. 그 뒤 같은 command ID로 Deny를 보내라. 과거 handle과 새 handle의 outcome을 비교하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

동일 승인만 한 번 소비된다. 다른 결정은 conflict다. 과거 handle은 이전 Waiting segment의 결과를 유지하고 새 handle이 계속된 실행을 관찰한다. Run ID는 같으며 이벤트 sequence는 이어진다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 17 --dest ../wickle-answer-17
python3 "$COURSE/lab.py" compare 17 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/17-resume.patch"
git apply "$COURSE/solutions/17-resume.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `d33573ed8be1f811f75a6dc116ea908a69917cd8`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-state-sqlite/tests/state_store.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/tools.rs`
- `crates/wickle/src/context_projection.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/input_binding.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/message.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/resume.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/tests/agent_resume.rs`
- `crates/wickle/tests/context_projection.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state_checkpoint.rs`
- `crates/wickle/tests/state_resume_invariants.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/agent_resume.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/agents.md`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
