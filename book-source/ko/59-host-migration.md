# 59장. 독립 Host 갱신과 저장 데이터 이관

[목차](README.md) · [이전](58-recovery-audit.md) · [다음](60-release-v02.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

58장의 검사를 마친 동일 실습 workspace에서 이어간다. 핵심 테스트가 통과해도 새 필수 필드 때문에 별도 Host 예제가 컴파일되지 않을 수 있다. 선택 consumer만 실행하면 다른 예제의 include 의존 깨짐도 놓친다.

이번 장의 정확한 checkpoint는 `1f8841fc027e0c6b9c6274760328333b15320006`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/59-host-migration.md)와 [정답 patch](solutions/59-host-migration.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. AgentBindings.interruption_policy, model binding.default_options, optional output cap, custom store ExecutionTransactions 등 Host 변경을 적용한다.

2. 공개 agent consumer에 layered options·custom maintenance AppState·stop/recover·old handle·inspect_step을 연결한다.

3. tool_schema consumer에서 native/strict 저장 계약과 optional 생략→default를 실제 실행한다.

4. check-package.py의 --consumer selector는 base 검증을 유지하고 해당 예제만 실행하게 한다. 최종에는 selector 없는 전체 검사를 실행한다.

먼저 `python3 "$COURSE/lab.py" inspect 59`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `tests/support/agent_consumer.rs`의 checkpoint 21행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        r: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(id("revision-1")),
                    ..r.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!("registered model")),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: None,
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn reference(name: &str) -> VersionedRef {
    VersionedRef {
        id: id(name),
        version: id("1"),
    }
}
fn routing_snapshot(scope: &Scope) -> Result<RoutingSnapshot, ContractError> {
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

튜토리얼도 실행 가능한 integration contract다. 짧은 선택 검사는 피드백을 빠르게 하지만 전체 패키지 검사를 대체하지 못한다. 특히 Rust include!로 예제 타입을 공유하면 별도 소비자의 필드 초기화까지 바꿔야 한다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test execution_contracts --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 59 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/execution_contracts.rs::interrupted_segments_require_matching_recoverable_evidence`
- `crates/wickle/tests/execution_contracts.rs::output_cap_preserves_omission_and_rejects_null_zero_or_fraction`
- `crates/wickle/tests/execution_contracts.rs::replay_uses_stored_canonicalization_instead_of_candidate_digest`

### 예측 → 결함 → 복구

agent 소비자의 fail 필드가 AtomicBool로 바뀌었는데 recovery consumer가 bool을 초기화하면 선택 agent 검사로 찾을 수 있는가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

선택 실행만으로는 못 찾을 수 있다. 실제 작업 리뷰에서 이 결함이 있었으므로 전체 소비자 빌드·실행이 필요하다. public cancel(reason, context)를 command ID API로 잘못 설명하지 않는 것도 확인한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 59 --dest ../wickle-answer-59
python3 "$COURSE/lab.py" compare 59 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/59-host-migration.patch"
git apply "$COURSE/solutions/59-host-migration.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/migration-v0.2.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
