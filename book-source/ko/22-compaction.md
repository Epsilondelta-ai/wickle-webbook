# 22장. 문맥 선택·미리보기·압축

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** ModelCompactor options:None을 agent Run 옵션 자동 상속으로 설명한 옛 규칙은 바뀐다. auxiliary purpose 설정을 독립적으로 고정하며 요약의 원천 권한도 다시 검사한다.

이어지는 구현: [42장](42-options.md) · [45장](45-fragments.md) · [46장](46-prepared-step.md).

[목차](README.md) · [이전 장](21-skills.md) · [다음 장](23-verification.md)

## 이번 장의 출발점과 결과

21장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **문맥 선택·미리보기·압축**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/22-compaction.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/22-compaction.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch10-01-syntax.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

모델 context window는 유한하다. Wickle은 먼저 완전한 기록 묶음을 선택하고 큰 결과의 preview를 적용한 뒤 필요한 경우에만 압축한다. 원본 transcript는 그대로 두고 ContextRevision을 새로 저장한다. 요약은 과거 데이터이므로 최신 요청 뒤에 명령처럼 놓지 않는다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 22
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. BoundedContextStrategy가 압축 가능한 완전한 message group을 선택하게 한다. 현재 요청·사용자 메시지·최근 도구 round·unknown 효과 등의 보호 경계를 지킨다.

2. preview artifact를 만들 때 deterministic identity를 사용해 동일 재시도가 원본을 중복 변경하지 않게 한다.

3. HostContextCompactor 또는 ModelCompactor를 연결한다. 모델 압축은 Compaction routing rule과 공통 ModelExchange/budget을 사용한다.

4. 후보 revision이 실제 byte를 줄이고 token estimate를 늘리지 않으며 최종 한도에 드는지 검사한다. 채택과 context.rewritten 이벤트를 원자적으로 저장한다.

## 실제 코드 읽기

`crates/wickle/src/context_strategy.rs`의 이 단계 20–51행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/22-compaction.md)에 있다.

```rust
pub struct ContextStrategyDefinition {
    /// Exact algorithm identity.
    pub strategy: VersionedRef,
    /// Schema for nonsecret profile context configuration.
    pub config_schema: Value,
}
/// One complete eligible conversation segment. Its IDs cannot be split on adoption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSegment {
    /// Original messages in order, never including protected user/control messages.
    pub message_ids: Vec<Id>,
    /// Bounded model-visible data, excluding opaque replay and private receipts.
    pub content: Value,
}
/// Read-only selection input; it exposes no store, mutable Run, or credentials.
#[derive(Clone)]
pub struct ContextSelectionInput {
    /// Owning namespace.
    pub scope: Scope,
    /// Current Run.
    pub run_id: Id,
    /// Fixed nonsecret context configuration.
    pub config: JsonObject,
    /// Complete eligible segments, oldest first.
    pub segments: Vec<ContextSegment>,
    /// Whether an existing cumulative summary can also be reduced.
    pub has_summary: bool,
    /// Maximum serialized selected data for a compactor request.
    pub max_input_bytes: usize,
}
impl fmt::Debug for ContextSelectionInput {
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

ContextStrategy는 교체 가능한 Strategy이고 compactor는 별도 알고리즘 Port다. selection, representation, semantic summarization을 분리하면 불필요한 LLM 비용을 피할 수 있다. 단점은 선택·압축 결과 검증 코드가 복잡하다는 것이다. 구조·크기 검증은 요약 문장의 사실성을 증명하지 않는다. 전체 원본 삭제 방식은 간단하지만 감사와 잘못된 요약 수정이 어려워진다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test agent_tool_loop --locked
python3 "$COURSE/lab.py" check 22 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/agent_tool_loop.rs` → `a_missing_compaction_route_is_rejected_before_agent_execution`
- `crates/wickle/tests/agent_tool_loop.rs` → `compaction_preserves_typed_artifact_and_evidence_anchors_from_removed_rounds`
- `crates/wickle/tests/contracts.rs` → `digest_matches_independent_sha256_vectors_and_sorts_nested_objects`
- `crates/wickle/tests/contracts.rs` → `ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized`

### 결함을 주입하는 연습

원본보다 긴 요약을 반환하고 compressor 호출 횟수를 검사하라. call만 선택하고 result를 빼는 전략을 주입하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

축소하지 못한 후보는 채택되지 않고 무제한 재압축하지 않는다. 불완전한 도구 쌍은 compressor 실행 전에 거절된다. 모델 압축을 별도 무료 호출처럼 구현하면 Run의 비용 한도가 무력화된다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 22 --dest ../wickle-answer-22
python3 "$COURSE/lab.py" compare 22 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/22-compaction.patch"
git apply "$COURSE/solutions/22-compaction.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `81464a6afadcb6350bab7dd918aa917eadcf5f41`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/artifacts.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/artifacts.rs`
- `crates/wickle/src/context_projection.rs`
- `crates/wickle/src/context_strategy.rs`
- `crates/wickle/src/context_strategy/compression.rs`
- `crates/wickle/src/context_strategy/engine.rs`
- `crates/wickle/src/context_strategy/model_compactor.rs`
- `crates/wickle/src/context_strategy/operations.rs`
- `crates/wickle/src/context_strategy/records.rs`
- `crates/wickle/src/context_strategy/runtime.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model_execution.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/serialization.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/checkpoint.rs`
- `crates/wickle/src/state/context_state.rs`
- `crates/wickle/src/state/hook_state.rs`
- `crates/wickle/src/views.rs`
- `crates/wickle/tests/agent_tool_loop.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/skills.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/agents.md`
- `docs/context-compaction.md`
- `docs/context.md`
- `tests/support/adapter_consumer.rs`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
- `tests/support/compaction_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/hooks_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/skills_consumer.rs`
- `tests/support/source_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
