# 09장. 모델 입력과 시스템 입력 분리

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** canonical schema와 provider wire schema를 분리한다. 숨은 system 의존 조건은 전체 실행 validator에 남기고 model 조건만 투영한다. provider 미지원만으로 Tool을 삭제하지 않는다.

이어지는 구현: [43장](43-provider-contracts.md) · [44장](44-argument-repair.md).

[목차](README.md) · [이전 장](08-model.md) · [다음 장](10-context.md)

## 이번 장의 출발점과 결과

08장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **모델 입력과 시스템 입력 분리**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/09-schema.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/09-schema.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch10-02-traits.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

도구의 전체 실행 schema와 모델이 채울 schema는 다르다. 검색 도구가 query와 workspace_id를 필요로 해도 모델에는 query만 맡길 수 있다. agent_parameters는 모델 소유 필드의 명시적 목록이다. 나머지는 등록된 시스템 입력과 연결한다. 모델이 workspace ID를 추측하게 만드는 설계는 권한 검증을 대신할 수 없다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 09
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. ToolDescriptor에 이름·버전·전체 입력/출력 schema·효과 분류·실행 한도를 정의한다.

2. SystemInputRegistry에 숨은 입력의 schema와 Run/Resolver 출처를 등록한다. compile은 값을 조회하지 않는다.

3. SchemaCompiler에서 모델 소유 속성만 투영한다. hidden 필드의 존재를 모델 인자로 허용하지 않고, agent_parameters=[]도 명시적으로 처리한다.

4. 로컬 참조만 허용하는 schema 검사를 작성한다. 원격 $ref는 네트워크에 접근해 해결하지 않는다. 입력 소유권을 가로지르는 제약은 안전한 투영이 가능한지 검증하고 불가능하면 거절한다.

## 실제 코드 읽기

`crates/wickle/src/tool_schema.rs`의 이 단계 23–54행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/09-schema.md)에 있다.

```rust
pub enum ToolSideEffect {
    /// The reviewed handler performs no external business writes.
    ReadOnly,
    /// The handler can change external business state.
    Write,
    /// Effects are not yet classified; no read-only assumptions are made.
    #[default]
    Unknown,
}

/// Reviewed concurrency capability, further restricted by runtime policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolConcurrency {
    /// Execute one call at a time.
    #[default]
    Serial,
    /// Read-only implementation reviewed for parallel calls.
    ParallelRead,
}

/// Declared retry safety; retry attempts still require runtime permission and budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryPolicy {
    /// No automatic retry is declared safe.
    #[default]
    Never,
    /// Reviewed read-only operation can be repeated.
    ReadOnly,
    /// The implementation honors the same external idempotency key on retry.
    Idempotent,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Compiler 패턴으로 선언적 descriptor를 실행 가능한 CompiledTool로 바꾼다. 검증 비용을 미리 지불하고 같은 계약을 재사용한다. 원본 descriptor digest와 compiler version을 보존해 복구 시 달라진 계약을 잡는다. 모든 JSON Schema 기능을 지원하면 표현력은 늘지만 안전한 부분 투영이 복잡해진다. 지원 부분집합을 명시적으로 제한하는 것이 이 버전의 선택이다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test tool_schema --locked
python3 "$COURSE/lab.py" check 09 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/tool_schema.rs` → `projection_validates_model_and_execution_inputs_at_separate_boundaries`
- `crates/wickle/tests/tool_schema.rs` → `agent_allowlist_must_be_present_explicit_unique_and_known`

### 결함을 주입하는 연습

query만 agent_parameters로 지정한 도구에 모델이 workspace_id까지 넣게 하라. 실제 Host 값과 같을 때도 통과하는지 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

같은 값이어도 모델의 숨은 필드 공급은 거절해야 한다. 값의 일치가 입력 소유권을 바꾸지 않는다. compiler는 권한을 부여하지 않으며 이후 binder와 PolicyGate가 여전히 필요하다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 09 --dest ../wickle-answer-09
python3 "$COURSE/lab.py" compare 09 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/09-schema.patch"
git apply "$COURSE/solutions/09-schema.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `80fb9dfeb63749cb634d3a5e9bae30a4ff6f866e`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/tool_schema.rs`
- `crates/wickle/tests/tool_schema.rs`
- `docs/contracts.md`
- `docs/tool-inputs.md`
- `tests/support/tool_schema_consumer.rs`

</details>
