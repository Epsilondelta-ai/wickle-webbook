# 05장. Scope와 현재 권한 검사

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 재개 요청자와 원래 execution principal을 분리한다. 진단도 현재 권한을 검사하며 read-only view가 만료 처리를 대신하지 않는다.

이어지는 구현: [48장](48-controls.md) · [49장](49-inspection.md).

[목차](README.md) · [이전 장](04-contracts.md) · [다음 장](06-state.md)

## 이번 장의 출발점과 결과

04장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Scope와 현재 권한 검사**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/05-policy.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/05-policy.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch13-00-functional-features.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Scope는 자원의 tenant/workspace/선택적 user 주소이고 principal은 지금 행동하는 주체다. 동일한 workspace에서 검토자가 바뀌어도 자원 주소가 바뀌지는 않는다. 권한 검사는 모델의 판단이 아니라 Host가 구현한 PolicyPort의 판단이다. Guarded는 Completed와 ApprovalRequired를 구분해서 승인이 필요한 상태를 성공값처럼 사용할 수 없게 한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 05
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. PolicyRequest의 action에 접근할 자원과 실제 인자를 담는다. 자원 owner scope는 신뢰하는 저장 상태에서 읽는다.

2. PolicyPort trait과 PolicyGate를 작성한다. 정확한 scope 비교를 먼저 하고, 현재 grant와 deadline을 전달해 authorize를 호출한다.

3. 실제 작업을 FnOnce closure로 받는다. allow가 확인된 뒤 closure를 호출해야 deny일 때 네트워크 객체 생성 등 선행 부작용도 막을 수 있다.

4. timeout·panic·취소·오류는 허용으로 변환하지 않는다. 공개 RunView와 보호된 details 조회를 분리한다.

## 실제 코드 읽기

`crates/wickle/src/policy.rs`의 이 단계 16–47행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/05-policy.md)에 있다.

```rust
pub struct ToolPolicyInput {
    /// Core call identity.
    pub call_id: Id,
    /// Exact tool identity and version.
    pub tool: VersionedRef,
    /// Pinned descriptor identity.
    pub descriptor_digest: JsonDigest,
    /// Binding identity computed by the trusted input binder.
    pub binding_digest: JsonDigest,
    execution_args: JsonObject,
}

impl ToolPolicyInput {
    /// Own the binder's final arguments. The gate never invents missing IDs.
    pub fn new(
        call_id: Id,
        tool: VersionedRef,
        descriptor_digest: JsonDigest,
        binding_digest: JsonDigest,
        execution_args: JsonObject,
    ) -> Self {
        Self {
            call_id,
            tool,
            descriptor_digest,
            binding_digest,
            execution_args,
        }
    }
    /// Inspect the full arguments to check actual target existence and ownership.
    pub fn execution_args(&self) -> &JsonObject {
        &self.execution_args
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

PolicyGate는 보호 프록시와 Guard의 역할을 한다. 정책 결정과 작업 수행을 분리하는 것은 단일 책임 원칙에 맞는다. closure로 작업 생성을 늦추는 방식이 핵심이며 단순히 실행 결과를 숨기는 것과 다르다. 매번 정책을 확인하면 철회에 대응하지만 지연이 추가된다. 정책 결과 캐시는 비용을 낮추는 대신 철회 창을 만든다. 타입과 in-process callback은 악성 코드 sandbox가 아니다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test policy --locked
python3 "$COURSE/lab.py" check 05 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/policy.rs` → `every_control_boundary_requires_exact_tenant_workspace_and_user_scope`
- `crates/wickle/tests/policy.rs` → `denial_errors_panics_timeout_and_cancellation_do_not_construct_operations`

### 결함을 주입하는 연습

정책을 Allow에서 Deny로 바꾼 뒤 동일 Run을 다시 읽어라. 또 user=None인 scope로 user=Some(...) 자원에 접근해 보라. 실제 읽기 callback 횟수를 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

이전 허용을 재사용하면 안 된다. None은 wildcard가 아니므로 scope도 거절된다. 거절된 작업의 callback 횟수는 0이어야 한다. 단지 화면에 AccessDenied를 출력하는 테스트로는 이 성질을 증명하지 못한다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 05 --dest ../wickle-answer-05
python3 "$COURSE/lab.py" compare 05 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/05-policy.patch"
git apply "$COURSE/solutions/05-policy.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `a8b4a269ad39362d37a028f3437db57f5e54f33b`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `.env.example`
- `.gitignore`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/policy.rs`
- `crates/wickle/src/views.rs`
- `crates/wickle/tests/policy.rs`
- `docs/policy.md`
- `docs/provider-setup.md`
- `scripts/check-package.py`
- `tests/support/policy_consumer.rs`

</details>
