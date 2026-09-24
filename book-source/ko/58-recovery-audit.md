# 58장. 경쟁·프로세스 종료·schema 확장의 통합 회귀

[목차](README.md) · [이전](57-mcp-repair.md) · [다음](59-host-migration.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

57장의 검사를 마친 동일 실습 workspace에서 이어간다. 깊이가 얕은 schema도 같은 local reference를 여러 번 확장하면 큰 중간 tree를 만들 수 있다. 최종 크기만 검사하면 이미 CPU·메모리를 소진한 뒤다. 단순 프로세스 재시작도 정확히 어느 저장 경계에서 죽었는지 알아야 한다.

이번 장의 정확한 checkpoint는 `1fbb643b1ae798b1312a305e9869b8a2a3b456b9`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/58-recovery-audit.md)와 [정답 patch](solutions/58-recovery-audit.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. Responses compiler의 모든 field/recursive lowering이 공유하는 node·byte 작업 예산을 확장 전에 차감한다. compiler revision 2를 고정하고 old contract는 그대로 restore한다.

2. 원자 admission·preparation 전후·예약 후, input wait·command acceptance 전후에 자식 process가 durable gate를 알리게 한다. 부모가 SIGKILL하고 새 owner가 복구한다.

3. same key/different payload 경쟁에서 winner snapshot만 남는지 Memory와 별도 SQLite connection에서 검증한다.

4. get_run의 deadline_expired는 권한 후 Clock 조회만 하며 terminal은 false다. 실패/시계 역행은 오류로 보존한다.

먼저 `python3 "$COURSE/lab.py" inspect 58`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-responses/src/schema.rs`의 checkpoint 8행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct ResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for ResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        let restricted = target
            .model
            .as_ref()
            .is_none_or(|model| model.id.as_str().starts_with("ft:"));
        compile(tool, target, SchemaPolicy::openai(restricted))
    }
}

/// Azure Responses projection using its documented schema subset and limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureResponsesToolSchemaCompiler;
impl ProviderToolSchemaCompiler for AzureResponsesToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-azure-responses-tool-schema").expect("constant"),
            version: Id::new("2").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

작업량 예산과 장애 경계 시험이다. depth limit만으로 계산량을 제한할 수 없으며 ready Future의 공정성과 caller poll frame 크기도 runtime 설계 일부다. 원자 transaction 내부의 불가능한 절반 상태를 test hook으로 발명하지 않는다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-state-sqlite --test agent_recovery --locked
cargo test -p wickle-state-sqlite --test state_store --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 58 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-state-sqlite/tests/agent_recovery.rs::compression_revision_process_boundaries_restore_only_complete_context_without_recompression`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs::forced_process_termination_preserves_admission_preparation_and_dispatch_reservations`
- `crates/wickle-state-sqlite/tests/agent_recovery.rs::forced_process_termination_never_half_consumes_an_input_command_or_replays_its_tool`
- `crates/wickle-state-sqlite/tests/state_store.rs::warmed_checkpoints_observe_external_updates_and_revalidate_changed_bytes`
- `crates/wickle-state-sqlite/tests/state_store.rs::a_failed_sql_write_never_publishes_mutated_cached_lease_state`
- `crates/wickle-state-sqlite/tests/state_store.rs::killing_an_uncommitted_legacy_upgrade_preserves_old_data_and_allows_one_later_upgrade`

### 예측 → 결함 → 복구

legacy upgrade 중단 테스트가 production migration 함수 내부에 hook을 심어 죽이는 테스트인가? fixture ModelPort 횟수는 실제 HTTP POST인가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

아니다. 실제 엔진으로 만든 v2 image를 미확정 SQLite transaction에 쓴 child를 kill하고 rollback·public admission 재시도를 검증한다. fixture port count는 adapter 진입 수이며 provider HTTP suite와 live 증거는 별도다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 58 --dest ../wickle-answer-58
python3 "$COURSE/lab.py" compare 58 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/58-recovery-audit.patch"
git apply "$COURSE/solutions/58-recovery-audit.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/recovery.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
