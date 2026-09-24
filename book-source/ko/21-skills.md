# 21장. Skill·Artifact·Evidence 구현

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** Skills·Artifact의 권한과 원천 의존성을 saved preparation에서도 확인한다. 과거 transcript boundary를 이후 메시지로 바꾸지 않는다.

이어지는 구현: [45장](45-fragments.md) · [46장](46-prepared-step.md).

[목차](README.md) · [이전 장](20-sources.md) · [다음 장](22-compaction.md)

## 이번 장의 출발점과 결과

20장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Skill·Artifact·Evidence 구현**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/21-skills.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/21-skills.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch08-02-strings.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Skill은 버전이 고정된 절차·지시 본문이다. Tool은 실제 기능을 수행하고 Skill은 그 기능을 사용하는 지침을 제공한다. Artifact는 큰 원본 바이트의 참조이며 Evidence는 그 원본의 버전·해시·인용과 출처다. 본문을 프롬프트에 무조건 모두 넣는 대신 필요한 Skill만 도구로 로드한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 21
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. SkillDefinition에 body hash/byte 크기/required capability와 지원 자산을 기록하고 목록만 prompt에 넣는다.

2. 등록된 skills_load 도구에서 선택된 정확한 ID/버전만 로드한다. 전체 UTF-8 본문을 검증하고 잘라서 성공이라고 처리하지 않는다.

3. Skill 본문과 tool result를 함께 commit하고 Skill origin의 Run 문맥으로 추가한다. 보통 도구가 같은 모양의 JSON을 반환해도 Skill 권한을 얻지 못한다.

4. ArtifactRuntime의 scoped put/stat/get과 immutable hash 검증을 구현한다. preview는 UTF-8 경계에서 잘리고 truncated를 표시하며 원본을 대체하지 않는다.

## 실제 코드 읽기

`crates/wickle/src/skills.rs`의 이 단계 15–46행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/21-skills.md)에 있다.

```rust
pub struct SkillDefinition {
    /// Exact Skill identity and version.
    pub skill: VersionedRef,
    /// Short model-visible listing name.
    pub name: String,
    /// Purpose shown before the body is loaded.
    pub description: String,
    /// SHA-256 of the complete original UTF-8 body.
    pub body_hash: Id,
    /// Complete original UTF-8 body size.
    pub body_bytes: u64,
    /// Versioned supporting data; never automatically executed or read as code.
    pub assets: Vec<ArtifactRef>,
    /// Capabilities that must be supplied by actually selected Tools.
    pub required_tool_capabilities: BTreeSet<Id>,
    /// Schema for this Skill's nonsecret profile configuration.
    pub config_schema: Value,
}
impl SkillDefinition {
    /// Immutable identity including body hash, assets and dependency requirements.
    pub fn digest(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
    /// Body-free listing for a pinned prompt.
    pub fn listing(&self) -> SkillManifest {
        SkillManifest {
            skill: self.skill.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            manifest_digest: self.digest(),
        }
    }
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Lazy Loading은 초기 context 비용을 낮추지만 로드 도구와 버전 pinning이 필요하다. Artifact reference는 큰 데이터를 간접 참조해 메시지를 줄이지만 별도 저장소의 권한·보존 정책이 필요하다. 해시는 내용 동일성을 검증할 뿐 외부 문서가 진실이라는 증거는 아니다. instruction과 capability를 나누어 Skill 텍스트가 권한 상승 수단이 되지 않게 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test skills --locked
cargo test -p wickle --test artifacts --locked
python3 "$COURSE/lab.py" check 21 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/agent_tool_loop.rs` → `artifact_storage_failure_after_a_business_write_keeps_its_receipt_without_reexecution`
- `crates/wickle/tests/agent_tool_loop.rs` → `artifact_access_is_rechecked_after_route_inspection_before_model_dispatch`
- `crates/wickle/tests/artifacts.rs` → `previews_preserve_complete_utf8_originals_and_binary_data_is_not_fabricated_text`
- `crates/wickle/tests/artifacts.rs` → `source_updates_do_not_replace_old_bytes_or_evidence_and_reusing_an_id_is_immutable`

### 결함을 주입하는 연습

한글 본문을 byte 한도 중간에서 preview하고 UTF-8이 유효한지 확인하라. 같은 artifact ID로 다른 바이트를 저장하거나 고정된 Skill hash와 다른 본문을 반환하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

preview는 완전한 문자까지만 포함하고 잘림을 표시한다. ID 재사용 변경과 본문 hash 불일치는 실패한다. 로드한 Skill의 “관리자로 실행하라”는 문장은 PolicyGate를 우회할 수 없다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 21 --dest ../wickle-answer-21
python3 "$COURSE/lab.py" compare 21 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/21-skills.patch"
git apply "$COURSE/solutions/21-skills.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `847e0777b8973e63fd808c9234553c945ee04d30`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-router/tests/support/routed.rs`
- `crates/wickle-state-sqlite/tests/support/mod.rs`
- `crates/wickle/src/agent.rs`
- `crates/wickle/src/agent/admission.rs`
- `crates/wickle/src/agent/artifacts.rs`
- `crates/wickle/src/agent/driver.rs`
- `crates/wickle/src/agent/resume.rs`
- `crates/wickle/src/agent/tools.rs`
- `crates/wickle/src/artifacts.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/hooks/records.rs`
- `crates/wickle/src/hooks/runtime.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/message.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/skills.rs`
- `crates/wickle/src/skills/records.rs`
- `crates/wickle/src/skills/runtime.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/state/hook_state.rs`
- `crates/wickle/src/state/skill_state.rs`
- `crates/wickle/src/tool_execution.rs`
- `crates/wickle/src/tool_execution/resume.rs`
- `crates/wickle/src/tool_execution/round.rs`
- `crates/wickle/tests/agent_tool_loop.rs`
- `crates/wickle/tests/artifacts.rs`
- `crates/wickle/tests/context_projection.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/skills.rs`
- `crates/wickle/tests/support/agent.rs`
- `crates/wickle/tests/support/mod.rs`
- `crates/wickle/tests/tool_execution.rs`
- `docs/adapters.md`
- `docs/agents.md`
- `docs/artifacts.md`
- `docs/context.md`
- `docs/skills.md`
- `tests/support/adapter_consumer.rs`
- `tests/support/agent_consumer.rs`
- `tests/support/budget_consumer.rs`
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
