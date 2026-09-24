# 08장. 모델 스트림과 물리 호출

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 완전한 응답 안의 잘못된 Tool 인자는 raw 원문과 repairable proposal로 보존한다. stream envelope 손상·미완료와 혼동하지 않는다. 준비 기록과 actual attempt도 구분한다.

이어지는 구현: [43장](43-provider-contracts.md) · [44장](44-argument-repair.md) · [46장](46-prepared-step.md) · [50장](50-openai-schema.md) · [52장](52-anthropic-repair.md).

[목차](README.md) · [이전 장](07-budget.md) · [다음 장](09-schema.md)

## 이번 장의 출발점과 결과

07장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **모델 스트림과 물리 호출**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/08-model.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/08-model.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch17-05-traits-for-async.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

모델의 스트림은 완성된 답변이 아니라 조각의 연속이다. 텍스트 조각, 도구 인자 조각, 사용량, 완료 신호를 받아 하나의 ModelResponse로 조립한다. 부분 JSON은 아직 실행 명령이 아니다. 전체 스트림이 일관되고 끝났다는 검사를 통과한 뒤에만 도구 계획이 될 수 있다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 08
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. ModelPort::generate가 한 물리 요청의 PortStream을 반환하게 한다. Port 내부에 agent loop나 자동 retry를 넣지 않는다.

2. collector에서 도구 call별 버퍼와 원래 순서를 관리한다. 동일 call ID 충돌, 완료 뒤 이벤트, 상충하는 완료 원인, 누락된 완료를 거절한다.

3. byte/event/tool 개수에 상한을 둔다. 빈 delta도 event 한도를 소비한다. 잘린 출력을 성공으로 취급하지 않는다.

4. ModelExchange를 policy·RunBudget·StateStore와 연결해 reservation, route, invocation, 응답 레코드를 저장한다. 실패 시 제한된 partial text와 완성되지 않은 tool plan을 구분한다.

## 실제 코드 읽기

`crates/wickle/src/model_protocol.rs`의 이 단계 21–52행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/08-model.md)에 있다.

```rust
pub struct ModelPortBinding {
    /// Registered service key, distinct for direct and hosted provider paths.
    pub provider: Id,
    /// Exact adapter implementation identity.
    pub adapter: VersionedRef,
    /// Host-owned credential/connection binding revision.
    pub connection_ref: VersionedRef,
}

impl ModelPortBinding {
    /// Require all adapter identities to match the immutable selected route.
    pub fn matches_route(&self, route: &ResolvedModelRoute) -> bool {
        self.provider == route.provider
            && self.adapter == route.adapter
            && self.connection_ref == route.connection_ref
    }
}

/// One physical model request's runtime context, without credentials or system inputs.
#[derive(Debug, Clone)]
pub struct ModelCallContext {
    /// Budget reservation and physical invocation identity.
    pub attempt_id: Id,
    /// Owning execution.
    pub run_id: Id,
    /// Authenticated data/execution scope supplied by the Host.
    pub scope: Scope,
    /// Cooperative cancellation signal.
    pub cancellation: CancellationToken,
    /// Effective deadline for this physical invocation.
    pub deadline: tokio::time::Instant,
}
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

ModelPort는 Port이며 각 모델 제공자 구현은 Adapter가 된다. collector는 wire parser와 엔진 실행기의 중간 프로토콜 경계다. 공통 이벤트로 정규화하면 테스트와 교체가 쉬워지지만 제공자 고유 기능을 공통 모델에 옮기는 비용이 생긴다. opaque continuation은 이 손실을 제한하되 정확한 원래 route에 묶는다. 다른 제공자에 그대로 보내는 범용 상태가 아니다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test model_protocol --locked
cargo test -p wickle --test model_execution --locked
python3 "$COURSE/lab.py" check 08 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/contracts.rs` → `digest_matches_independent_sha256_vectors_and_sorts_nested_objects`
- `crates/wickle/tests/contracts.rs` → `ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized`
- `crates/wickle/tests/model_execution.rs` → `each_retry_has_its_own_saved_attempt_and_rechecks_policy`
- `crates/wickle/tests/model_execution.rs` → `exhausted_recovery_and_model_budgets_each_stop_new_requests`

### 결함을 주입하는 연습

도구 인자 {"amount": 에서 스트림을 끊어라. 텍스트가 이미 일부 왔더라도 tool executor 호출 횟수가 0인지 검사하라. Completed 다음에 delta를 더 보내 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

불완전한 응답은 오류와 제한된 partial text로 남고 실행 가능한 호출 계획은 없어야 한다. terminal 뒤 delta도 프로토콜 위반이다. HTTP 200만으로 의미적으로 완료된 응답을 증명할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 08 --dest ../wickle-answer-08
python3 "$COURSE/lab.py" compare 08 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/08-model.patch"
git apply "$COURSE/solutions/08-model.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `d910e8cd8268c42235b1a9157016c548e860d628`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `crates/wickle/src/budget.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/model_execution.rs`
- `crates/wickle/src/model_protocol.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/model_execution.rs`
- `crates/wickle/tests/model_protocol.rs`
- `docs/contracts.md`
- `scripts/check-package.py`
- `tests/support/model_consumer.rs`

</details>
