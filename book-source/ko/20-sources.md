# 20장. 검색과 메모리를 ContextSource로 연결

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** core_revision/content_digest/source_revision을 분리하고 Empty·Unavailable·Deleted와 lineage의 의미를 구분한다. 삭제된 원천을 요약으로 우회할 수 없다.

이어지는 구현: [45장](45-fragments.md).

[목차](README.md) · [이전 장](19-adapters.md) · [다음 장](21-skills.md)

## 이번 장의 출발점과 결과

19장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **검색과 메모리를 ContextSource로 연결**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/20-sources.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/20-sources.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

ContextSource는 자동 검색·메모리 조회를 수행하는 read-only Port다. 모델이 선택하는 업무 Tool과 다르다. Ready는 자료 있음, Empty는 성공했지만 없음, Unavailable은 운영상 조회 실패다. required source가 Empty라고 반드시 실패할 필요는 없지만 Unavailable이면 필요한 조회를 수행하지 못한 것이다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 20
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. source 정의와 trigger(run_start/before_model), item/byte/token/timeout 한도를 고정한다.

2. provide에 원래 요청과 제한된 실행 context만 넘긴다. source별 local ID에 namespace를 부여해 충돌을 막고 provenance를 검증한다.

3. ContextBatch를 저장한 뒤 projection에 사용한다. 같은 logical step의 physical retry는 재조회하지 않는다.

4. authorize_use로 cached batch의 현재 접근권을 확인한다. 이후 Empty/Unavailable이 나오면 활성 슬롯을 교체하고 예전 Ready 결과를 되살리지 않는다.

## 실제 코드 읽기

`crates/wickle/src/context_source.rs`의 이 단계 14–45행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/20-sources.md)에 있다.

```rust
pub struct ContextSourceDefinition {
    /// Native source identity and exact version.
    pub source: VersionedRef,
    /// Only Retrieval and Memory are valid automatic-source origins.
    pub origin: ContextOrigin,
    /// Supported source contract version, currently 1.
    pub contract_version: u32,
}
impl ContextSourceDefinition {
    /// Complete definition identity.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Reject privileged origins or unknown contracts.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract_version != 1 {
            return Err(source_error(
                ErrorCode::UnsupportedContractVersion,
                "context_source.definition",
            ));
        }
        if !matches!(
            self.origin,
            ContextOrigin::Retrieval | ContextOrigin::Memory
        ) {
            return Err(source_error(
                ErrorCode::InvalidContract,
                "context_source.origin",
            ));
        }
        Ok(())
    }
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

읽기 전략을 ContextSource Port로 분리해 RAG 저장소와 memory 서비스를 교체할 수 있다. batch snapshot은 실행 재현성을 높이지만 실시간 최신 자료를 매 retry에 반영하지 않는다. 새 logical step이나 Run에서 다시 조회하는 경계가 필요하다. optional은 운영상 실패를 허용한다는 뜻이지 schema·권한·scope 위반을 무시한다는 뜻이 아니다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-adapter-runtime --test context_sources --locked
cargo test -p wickle --test context_sources --locked
python3 "$COURSE/lab.py" check 20 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-adapter-runtime/tests/agent_components.rs` → `approval_wait_releases_all_instances_and_resume_uses_new_bindings_with_the_frozen_mapping`
- `crates/wickle-adapter-runtime/tests/agent_components.rs` → `initialization_and_dispatch_permissions_remain_separate_for_adapter_exports`
- `crates/wickle-adapter-runtime/tests/context_sources.rs` → `one_catalog_source_can_serve_both_triggers_without_duplicate_runtime_registration`
- `crates/wickle-adapter-runtime/tests/context_sources.rs` → `adapter_source_batch_survives_waiting_while_fresh_instances_reauthorize_its_original_items`

### 결함을 주입하는 연습

첫 조회 Ready, 다음 논리 step 조회 Empty로 만든다. 두 번째 모델 요청에 첫 자료가 남는지 검사하라. 저장된 batch에 대한 권한을 retry 직전에 철회하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

Empty로 바뀐 슬롯에서 과거 자료를 보내면 안 된다. cached 자료와 그 자료에서 파생된 Hook 데이터도 현재 권한이 없으면 모델에 전달되지 않는다. 수집 시 허용받았다는 사실만으로 미래 사용을 승인할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 20 --dest ../wickle-answer-20
python3 "$COURSE/lab.py" compare 20 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/20-sources.patch"
git apply "$COURSE/solutions/20-sources.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `f0c6202267c3135ebfa87e3963ba747890629118`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-adapter-runtime/src/lib.rs`
- `crates/wickle-adapter-runtime/src/lifecycle.rs`
- `crates/wickle-adapter-runtime/src/registry.rs`
- `crates/wickle-adapter-runtime/src/runtime.rs`
- `crates/wickle-adapter-runtime/tests/agent_components.rs`
- `crates/wickle-adapter-runtime/tests/context_sources.rs`
- `crates/wickle-adapter-runtime/tests/runtime_registry.rs`
- `crates/wickle-adapter-runtime/tests/support/context_sources.rs`
- `crates/wickle-adapter-runtime/tests/support/mod.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/components.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/sources.rs`
- `crates/wickle/src/component_runtime.rs`
- `crates/wickle/src/context_source.rs`
- `crates/wickle/src/context_source/records.rs`
- `crates/wickle/src/context_source/runtime.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/hooks/records.rs`
- `crates/wickle/src/hooks/runtime.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model_execution.rs`
- `crates/wickle/src/model_execution/routed.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/resolution.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/hook_state.rs`
- `crates/wickle/src/state/source_state.rs`
- `crates/wickle/tests/context_sources.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state_sources.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/context_sources.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/adapters.md`
- `docs/agents.md`
- `docs/context-sources.md`
- `docs/context.md`
- `docs/hooks.md`
- `tests/support/adapter_consumer.rs`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/hooks_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/resume_consumer.rs`
- `tests/support/routing_consumer.rs`
- `tests/support/source_consumer.rs`
- `tests/support/sqlite_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_loop_consumer.rs`

</details>
