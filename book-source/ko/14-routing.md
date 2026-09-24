# 14장. 정확한 모델 선택과 제한된 fallback

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 같은 입력 retry는 저장 preparation을 재사용한다. fallback은 새로운 projection revision으로 남기고 대상 schema를 다시 검증한다. 반복 retry를 SDK에 숨기지 않는다.

이어지는 구현: [42장](42-options.md) · [46장](46-prepared-step.md).

[목차](README.md) · [이전 장](13-sqlite.md) · [다음 장](15-agent.md)

## 이번 장의 출발점과 결과

13장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **정확한 모델 선택과 제한된 fallback**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/14-routing.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/14-routing.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch18-02-trait-objects.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Router는 목적과 제약에 맞는 route를 선택하고 Dispatcher는 그 route의 정확한 adapter 인스턴스를 찾는다. Exchange는 실제 호출·예산·권한·저장을 관리한다. 선택과 실행을 나누면 “어떤 모델을 쓸까” 정책을 바꾸어도 실패 복구와 도구 실행 규칙을 다시 쓰지 않아도 된다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 14
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. RoutingPolicy의 rule마다 목적·primary·유한 fallback·허용 실패 종류를 고정한다.

2. PolicyModelRouter에서 후보의 capability·options·version·support를 검사한다. RegistryModelDispatcher는 scope/provider/adapter/connection 전체 키로 조회한다.

3. ModelRouteInspector로 현재 메타데이터를 관찰하고 고정된 route와 비교한다. 모르는 version을 요청값으로 채워 넣지 않는다.

4. generate_routed에서 논리 step 입력과 routing snapshot을 저장한다. fallback 때 새 projection과 physical attempt를 만들되 같은 Run budget을 사용한다.

## 실제 코드 읽기

`crates/wickle-model-router/src/routing.rs`의 이 단계 10–41행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/14-routing.md)에 있다.

```rust
pub struct PolicyModelRouter {
    snapshot: RoutingSnapshot,
    catalog: ImmutableModelCatalog,
}

impl PolicyModelRouter {
    /// Own one validated snapshot. A later catalog object cannot replace its data.
    pub fn new(snapshot: RoutingSnapshot) -> Result<Self, ContractError> {
        let catalog = ImmutableModelCatalog::new(snapshot.catalog().clone())?;
        Ok(Self { snapshot, catalog })
    }

    async fn select(&self, request: &RouteRequest) -> Result<RouteSelection, ContractError> {
        if &request.scope != self.snapshot.scope() {
            return Err(error(ErrorCode::AccessDenied, "routing.scope"));
        }
        let rule = self
            .snapshot
            .policy()
            .rules
            .iter()
            .find(|rule| {
                rule.model_binding == request.model_binding && rule.purpose == request.purpose
            })
            .ok_or_else(|| error(ErrorCode::ModelRouteDenied, "routing.rule"))?;
        if rule.min_support < ModelSupportStatus::ContractTested {
            return Err(error(
                ErrorCode::ModelSupportInsufficient,
                "routing.min_support",
            ));
        }
        let candidates: Vec<_> = std::iter::once(&rule.primary)
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Router는 Strategy, Dispatcher는 Registry, 제공자 경계는 Adapter다. 이 세 가지를 하나의 함수에 섞으면 fallback 분기마다 예산·정책 누락이 생기기 쉽다. 분리는 독립 테스트를 가능하게 하지만 객체 조립과 인자 전달량을 늘린다. 고정 단일 모델만 필요하면 FixedModelRouter가 더 간단하다. fallback은 availability를 높이지만 다른 지역·비용·데이터 처리 경로로 이동할 수 있으므로 자동 임의 선택을 금지한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-router --test routed_execution --locked
python3 "$COURSE/lab.py" check 14 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-router/tests/dispatcher.rs` → `exact_scope_provider_adapter_and_connection_revisions_select_independent_ports`
- `crates/wickle-model-router/tests/dispatcher.rs` → `missing_exact_keys_never_fall_back_and_duplicate_registration_is_rejected`
- `crates/wickle-model-router/tests/routed_execution.rs` → `retry_and_fallback_share_saved_budgets_and_keep_exact_accounts_and_inspection_evidence`
- `crates/wickle-model-router/tests/routed_execution.rs` → `denied_fallback_is_neither_projected_nor_inspected_nor_dispatched`

### 결함을 주입하는 연습

RateLimited에는 두 번째 후보를 허용하고 Authentication에는 허용하지 않도록 rule을 만든다. 두 번째 후보의 현재 권한을 철회한 뒤 projection/inspection/dispatch 횟수를 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

RateLimited만 설정된 순서로 이동한다. denied 후보는 세 작업 모두 시작하지 않는다. 완료된 동일 step은 저장된 응답을 재사용하지만 현재 권한과 입력 동일성 검사는 계속 필요하다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 14 --dest ../wickle-answer-14
python3 "$COURSE/lab.py" compare 14 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/14-routing.patch"
git apply "$COURSE/solutions/14-routing.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `1f36da1996dffb48a6eee15de40ff477a133803f`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `crates/wickle-model-router/Cargo.toml`
- `crates/wickle-model-router/src/dispatcher.rs`
- `crates/wickle-model-router/src/lib.rs`
- `crates/wickle-model-router/src/routing.rs`
- `crates/wickle-model-router/tests/dispatcher.rs`
- `crates/wickle-model-router/tests/routed_execution.rs`
- `crates/wickle-model-router/tests/routing.rs`
- `crates/wickle-model-router/tests/support/routed.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/model_dispatch.rs`
- `crates/wickle/src/model_execution.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/model_protocol.rs`
- `crates/wickle/src/model_routing.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/model-catalog.md`
- `docs/model-routing.md`
- `tests/support/budget_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`

</details>
