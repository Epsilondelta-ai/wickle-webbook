# 37장. 호환성: 라이브러리 API와 저장 형식은 다르다

[목차](README.md) · [이전](36-release.md) · [다음](38-canonical-json.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

36장의 검사를 마친 동일 실습 workspace에서 이어간다. 새 crate가 빌드된다는 사실과 예전 실행 데이터를 안전하게 이어받을 수 있다는 사실은 다르다. v0.1 worker가 진행 중인 결제를 두고 새 runtime을 교체하면, 새로 추가된 segment·원래 실행자·명령 이력이 없어서 복구 판단에 필요한 근거가 부족하다.

이번 장의 정확한 checkpoint는 `0a5b2b5be6c9c86222cf84e3f14ba41b5a326efc`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/37-compatibility.md)와 [정답 patch](solutions/37-compatibility.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. 먼저 v0.1.0 terminal과 nonterminal checkpoint를 구분하고 변경된 public struct literal·enum match·Port 구현을 목록으로 만든다.

2. 라이브러리 버전, 저장 schema ID, canonicalization version, provider model version을 서로 독립된 축으로 표에 적는다.

3. terminal은 원래 outcome과 digest를 그대로 읽고 active는 원 runtime으로 drain한 뒤 이관한다는 계약을 문서화한다. 코드 없이도 이후 구현의 오류 의미가 달라지는 중요한 단계다.

먼저 `python3 "$COURSE/lab.py" inspect 37`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `docs/compatibility.md`의 checkpoint 1행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```text
# Runtime compatibility

Wickle has separate compatibility boundaries for Rust source code, serialized profiles, and persisted executions. A package version alone does not establish that a saved execution can be resumed by a different runtime.

## Rust applications and custom stores

Applications may construct public request structs directly and exhaustively match public status enums. Adding a struct field or an enum variant can therefore require application changes, even when a JSON field has a default. Adding a required `StateStore` method also requires changes to custom store implementations.

Before upgrading, build the application and its custom ports against the selected version. Check both serialization and actual execution paths. Do not implement new atomic storage operations as separate read/write calls merely to satisfy a trait: command consumption, execution ownership and state transitions must preserve the guarantees of the target runtime.

## Stored data

Keep a backup before changing a persistent store. Check the snapshot, event and database schema versions independently. Preserve the original request comparison rules and digest version. Re-encoding an old request with a new serializer is not evidence that it is the same submitted request.

An upgrade procedure must distinguish these cases:

| Saved execution | Required handling |
| --- | --- |
| Terminal | Preserve the original outcome and event history; do not execute the request again. |
| Not dispatched | Resume only if the original execution configuration and input contract can be reconstructed. |
| Dispatching or unknown effect | Reconcile the external effect before considering another attempt. |
| Waiting for approval or input | Preserve the wait identity, target and saved tool binding. An answer must not change the original execution principal. |
| Missing required historical information | Use a documented migration or finish the execution with the original runtime. Do not invent missing revisions or assume that an external operation did not run. |

A successful database open or JSON decode is not a successful recovery test. Verify the number of actual external effects, the returned outcome and the stored event history after restarting a separate process.

## Upgrade support

No automatic migration to a future runtime format is promised by this guide. Use the migration and compatibility instructions shipped with the target release. Where an active execution cannot be migrated safely, stop accepting new work, resolve or finish active executions with the original runtime, and then upgrade using a backed-up store.

Do not start an older runtime against a store that has written a newer format unless downgrade support is explicitly documented. Restoring a database backup does not undo external tool effects; reconcile those effects before resuming restored executions.

See [state storage](state.md), [SQLite storage](sqlite-state-store.md), and [recovery](recovery.md) for the current runtime contracts.
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

호환성은 하나의 boolean이 아니라 source·wire·state·behavior별 계약이다. 별도 구 API 실행기를 유지하지 않아 새 API는 일관되지만 Host는 재빌드해야 한다. 저장 자료가 부족한 상태를 자동 보정하는 대신 명시적으로 거부하는 것은 가용성보다 중복 업무 방지를 우선한 선택이다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle --test contracts --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 37 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle/tests/contracts.rs::success_requires_a_matching_completion_basis_and_verified_success_requires_evidence`
- `crates/wickle/tests/contracts.rs::event_and_input_contracts_reject_unsupported_versions_and_execution_injection`
- `crates/wickle/tests/contracts.rs::route_roundtrip_keeps_model_api_deployment_and_adapter_versions_distinct`

### 예측 → 결함 → 복구

Interrupted variant가 새로 생겼다는 이유로 모든 과거 Waiting을 Interrupted로 바꾸어 저장하면 왜 안 되는가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

그 당시 없었던 실행 소유자·segment·승인/명령 근거를 발명하게 된다. 원 상태의 효과와 승인 대상을 보존하고 구 runtime으로 정리하거나 명시적 미지원 오류를 반환해야 한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 37 --dest ../wickle-answer-37
python3 "$COURSE/lab.py" compare 37 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/37-compatibility.patch"
git apply "$COURSE/solutions/37-compatibility.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/migration-v0.2.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
