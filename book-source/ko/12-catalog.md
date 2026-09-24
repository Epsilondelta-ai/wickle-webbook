# 12장. 모델 카탈로그·버전·옵션

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** ModelBinding.default_options → AgentProfile.model_options → RunRequest.model_options의 최상위 교체와 출처·schema revision을 저장한다.

이어지는 구현: [42장](42-options.md).

[목차](README.md) · [이전 장](11-binding.md) · [다음 장](13-sqlite.md)

## 이번 장의 출발점과 결과

11장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **모델 카탈로그·버전·옵션**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/12-catalog.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/12-catalog.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch08-01-vectors.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

모델 이름, 실제 모델 버전, API 버전, 어댑터 버전, credential 연결 revision, 클라우드 deployment는 다른 식별자다. “같은 GPT”라는 말만으로 동일 실행을 재현할 수 없다. ModelDefinition은 모델의 성질을, ModelBinding은 그 모델을 실제 연결하는 조합을 나타낸다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 12
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. ModelDefinition/Capabilities/Binding을 작성하고 scope와 revision을 가진 catalog snapshot을 만든다.

2. ImmutableModelCatalog에 정확한 조회와 직접 alias 해석을 구현한다. alias의 alias로 끝없이 추적하지 않는다.

3. 기능·옵션 schema·context/output 한도를 모델과 binding 양쪽에서 검사한다. 지원하지 않는 옵션은 지우지 말고 오류로 돌려준다.

4. support evidence를 정확한 contract digest에 묶는다. 이 단계의 후속 변경으로 model_options가 admission digest부터 projection까지 유지되도록 연결한다.

## 실제 코드 읽기

`crates/wickle/src/model_catalog.rs`의 이 단계 17–48행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/12-catalog.md)에 있다.

```rust
pub struct ModelDefinitionRef {
    /// Registered service path, without a closed provider enum.
    pub provider: Id,
    /// Host catalog key, distinct from the provider model identifier.
    pub model_key: Id,
    /// Exact version selected within this key and provider.
    pub model_version: Id,
}

/// Lifecycle known at this catalog revision, not a live availability probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelLifecycle {
    /// Available according to supplied metadata.
    Active,
    /// Still usable but marked for replacement.
    Deprecated,
    /// Retired; invocation validation rejects it without finding a replacement.
    Retired,
    /// Known unavailable in this environment.
    Unavailable,
}

/// Informational evidence supplied by the trusted Host; it is not fetched here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEvidence {
    /// Host record or documentation reference supporting this metadata.
    pub source_ref: Id,
    /// UTC milliseconds when the metadata was checked.
    pub observed_at_ms: i64,
}
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

불변 Catalog와 명시적 Registry를 사용한다. provider를 거대한 enum으로 제한하지 않아 새 제공자 메타데이터를 등록하기 쉽다. 대신 문자열 식별자는 오타를 컴파일러가 잡지 못하므로 등록 검증과 정확한 lookup이 중요해진다. mutable alias를 허용하면 운영 편의가 생기지만 같은 이름의 실행 재현성이 약해진다. RequirePinned는 이 tradeoff를 명시적으로 선택하게 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-router --test catalog --locked
python3 "$COURSE/lab.py" check 12 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-router/tests/catalog.rs` → `the_same_provider_and_model_id_keep_two_releases_and_binding_revisions_available`
- `crates/wickle-model-router/tests/catalog.rs` → `provider_api_deployment_and_adapter_combinations_do_not_collapse_into_one_model_identity`
- `crates/wickle/tests/context_projection.rs` → `host_model_options_reach_the_port_without_becoming_prompt_content`
- `crates/wickle/tests/context_projection.rs` → `host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas`

### 결함을 주입하는 연습

같은 model_id의 두 model_version을 함께 등록하고 각각 다른 option schema를 준다. 첫 버전의 성공 evidence를 두 번째 binding에 붙여 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

두 버전은 함께 존재해야 하고 잘못된 schema 옵션은 거절된다. 다른 계약 digest의 evidence는 지원 승격 근거가 될 수 없다. 요청한 버전을 provider가 실제 반환했다고 복사해 채우는 것도 금지다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 12 --dest ../wickle-answer-12
python3 "$COURSE/lab.py" compare 12 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/12-catalog.patch"
git apply "$COURSE/solutions/12-catalog.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `3fdf792ffef46db13679472f3116b8f252e1550a`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `.env.example`
- `Cargo.lock`
- `Cargo.toml`
- `crates/wickle-model-router/Cargo.toml`
- `crates/wickle-model-router/src/lib.rs`
- `crates/wickle-model-router/tests/catalog.rs`
- `crates/wickle/src/context_projection.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/model_catalog.rs`
- `crates/wickle/src/model_protocol.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/tests/context_projection.rs`
- `crates/wickle/tests/contracts.rs`
- `crates/wickle/tests/model_execution.rs`
- `crates/wickle/tests/model_protocol.rs`
- `crates/wickle/tests/policy.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/mod.rs`
- `crates/wickle/tests/tool_schema.rs`
- `docs/context.md`
- `docs/contracts.md`
- `docs/env/README.md`
- `docs/env/anthropic.md`
- `docs/env/azure-openai.md`
- `docs/env/bedrock.md`
- `docs/env/gemini.md`
- `docs/env/openai.md`
- `docs/env/vertex-ai.md`
- `docs/env/xai.md`
- `docs/model-catalog.md`
- `scripts/check-package.py`
- `tests/support/budget_consumer.rs`
- `tests/support/catalog_consumer.rs`
- `tests/support/context_consumer.rs`
- `tests/support/input_binding_consumer.rs`
- `tests/support/model_consumer.rs`
- `tests/support/state_consumer.rs`
- `tests/support/tool_schema_consumer.rs`

</details>
