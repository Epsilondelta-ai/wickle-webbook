# 11장. 도구 실행 인자를 확정하고 저장하기

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 0.2.0에서는 누락된 최상위 모델 필드의 direct/local-ref default를 required 여부와 무관하게 먼저 적용한다. 명시 null·중첩·system default는 추정하지 않는다. 재개 reviewer가 원 execution actor를 대체하지 않는다.

이어지는 구현: [44장](44-argument-repair.md) · [48장](48-controls.md).

[목차](README.md) · [이전 장](10-context.md) · [다음 장](12-catalog.md)

## 이번 장의 출발점과 결과

10장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **도구 실행 인자를 확정하고 저장하기**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/11-binding.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/11-binding.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch08-03-hash-maps.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

InputBinder는 모델 인자와 Host의 시스템 값으로 최종 실행 인자를 만든 뒤 저장한다. 승인 화면에 보인 대상과 실제 실행 대상이 같으려면 이 값이 고정되어야 한다. resolver가 “최근 보고서 ID”를 반환하고 승인 동안 최신 보고서가 바뀌어도 이미 승인한 call의 대상은 바뀌면 안 된다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 11
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. RunSystemInputs::capture로 등록된 Run 입력만 snapshot에 담는다. resolver 소유 키를 Run 값으로 덮어쓰지 못하게 한다.

2. 모델 인자를 먼저 검증하고 지원되는 누락 optional 최상위 default만 적용한다. required 값, 명시적 null, 중첩 값, 시스템 ID를 지어내지 않는다.

3. 선택된 resolver key만 현재 정책 아래 조회하고 별칭은 한 번의 조회를 공유한다. 전체 system map을 executor로 넘기지 않는다.

4. BoundToolInput과 call의 참조를 원자적으로 저장한다. 재호출하면 기존 record와 digest를 검사하고 현재 권한만 다시 확인한다. resolver를 다시 실행하지 않는다.

## 실제 코드 읽기

`crates/wickle/src/input_binding.rs`의 이 단계 29–60행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/11-binding.md)에 있다.

```rust
pub struct InputBindingLimits {
    /// Maximum distinct resolver keys read for one new call; zero disables resolver reads.
    pub max_resolver_calls: usize,
    /// Maximum serialized bytes in one resolved or run-supplied value.
    pub max_value_bytes: usize,
    /// Maximum protected run-input or bound-input record size.
    pub max_bound_bytes: usize,
}
impl Default for InputBindingLimits {
    fn default() -> Self {
        Self {
            max_resolver_calls: 64,
            max_value_bytes: 65_536,
            max_bound_bytes: 1_048_576,
        }
    }
}
impl InputBindingLimits {
    fn validate(self) -> Result<(), ContractError> {
        if self.max_value_bytes == 0 || self.max_bound_bytes == 0 {
            return Err(error(ErrorCode::InvalidContract, "input_binding.limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunInputData {
    schema_version: String,
    scope: Scope,
    values: SystemInputs,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Immutable Snapshot으로 시간에 따라 변하는 조회 결과를 실행 계약에 고정한다. 이는 TOCTOU, 즉 확인 시점과 사용 시점 사이에 대상이 달라지는 문제를 줄인다. 고정된 값은 재현성을 주지만 사용자가 최신 대상으로 바꾸고 싶다면 새 call이 필요하다. 저장한 값의 동일성과 현재 권한은 별개이므로 immutable하다는 이유로 접근 재검사를 생략하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test input_binding --locked
python3 "$COURSE/lab.py" check 11 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/input_binding.rs` → `run_capture_rejects_unregistered_keys_invalid_values_and_resolver_source_conflicts`
- `crates/wickle/tests/input_binding.rs` → `restored_run_inputs_distinguish_omission_from_empty_or_changed_resume_values`
- `crates/wickle/tests/state.rs` → `identical_retries_return_the_original_run_without_replacing_resolved_metadata`
- `crates/wickle/tests/state.rs` → `concurrent_duplicate_admission_creates_exactly_one_run`

### 결함을 주입하는 연습

resolver가 처음에는 report-A, 다음에는 report-B를 돌려주도록 만든다. 같은 call을 두 번 bind하고 새 call도 bind하라. resume에 system_inputs를 생략하거나 빈 map을 보내 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

기존 call은 A를 재사용하고 새로운 call은 B를 얻을 수 있다. resume 생략은 원래 입력 재사용이고 명시적 빈 map은 실제 빈 값 주장이다. 원래 값이 있었다면 빈 map으로 바꾸는 것은 conflict다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 11 --dest ../wickle-answer-11
python3 "$COURSE/lab.py" compare 11 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/11-binding.patch"
git apply "$COURSE/solutions/11-binding.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `970ccc47a74e527f6ebbf6c4561c5f49eb0b308d`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `.env.example`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/input_binding.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/state.rs`
- `crates/wickle/src/tool_schema.rs`
- `crates/wickle/tests/input_binding.rs`
- `crates/wickle/tests/state.rs`
- `crates/wickle/tests/support/mod.rs`
- `docs/contracts.md`
- `docs/env/README.md`
- `docs/env/anthropic.md`
- `docs/env/azure-openai.md`
- `docs/env/gemini.md`
- `docs/env/openai.md`
- `docs/env/xai.md`
- `docs/input-binding.md`
- `docs/provider-setup.md`
- `docs/tool-inputs.md`
- `tests/support/input_binding_consumer.rs`

</details>
