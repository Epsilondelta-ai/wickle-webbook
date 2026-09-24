# 57장. MCP 전체 도구 루프의 수정과 Unknown

[목차](README.md) · [이전](56-xai-contract.md) · [다음](58-recovery-audit.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

56장의 검사를 마친 동일 실습 workspace에서 이어간다. MCP 도구도 같은 Agent의 저장된 provider decode·canonical 검증·system binding을 거쳐야 한다. executor를 직접 호출하는 테스트만으로 전체 입력 경계를 검증할 수 없다.

이번 장의 정확한 checkpoint는 `d4d6105fbaff87a2a6a452931f9ef543e538f284`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/57-mcp-repair.md)와 [정답 patch](solutions/57-mcp-repair.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. 기존 MCP runtime을 재사용한다. 새로운 중복 schema 계층을 만들지 않는다.

2. Agent→model proposal→saved contract decode→default/validation→system UUID→실제 stdio server 경로를 연결한다.

3. malformed JSON, hidden key 위조, JsonText 오류, type 위반에서 server 호출 0회를 확인한다.

4. 유효 호출 1회와 결과 replay, write 후 child exit·실행 중 cancel의 Unknown·cleanup을 검사한다.

먼저 `python3 "$COURSE/lab.py" inspect 57`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-mcp/src/executor.rs`의 checkpoint 7행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct McpToolExecutor {
    pub(crate) client: McpClient,
    pub(crate) snapshot: McpSnapshot,
    pub(crate) remote: String,
    pub(crate) compiled: CompiledTool,
    pub(crate) segment: Option<(Id, Id)>,
}
impl McpClient {
    /// Bind an explicitly reviewed/compiled descriptor. This never chooses exposure
    /// or permissions from remote annotations and never activates another Tool.
    pub fn bind_tool(
        &self,
        snapshot: &McpSnapshot,
        remote: &str,
        compiled: CompiledTool,
    ) -> Result<McpToolExecutor, ContractError> {
        if snapshot.scope() != self.scope() || snapshot.connection_ref() != self.connection_ref() {
            return Err(error(ErrorCode::AccessDenied, "snapshot_scope"));
        }
        snapshot.validate_descriptor(remote, compiled.descriptor())?;
        Ok(McpToolExecutor {
            client: self.clone(),
            snapshot: snapshot.clone(),
            remote: remote.into(),
            compiled,
            segment: None,
        })
    }
}
fn failed(code: &str, effect: ToolEffect) -> ToolExecutionResult {
    ToolExecutionResult {
        outcome: ToolExecutionOutcome::Failed {
            code: Id::new(code).expect("static code"),
        },
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

통합 contract test가 기존 abstraction의 일관성을 증명한다. 새 기능 이름이 생겼다고 adapter를 재작성하지 않는 surgical change다. remote success 문자열은 trusted business receipt가 아니므로 기존 Unknown 정책은 유지된다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-mcp --test stdio --locked
cargo test -p wickle-mcp --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 57 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-mcp/tests/stdio.rs::bounded_discovery_and_notification_flood_fail_closed`
- `crates/wickle-mcp/tests/stdio.rs::content_projection_and_output_limits_are_enforced`
- `crates/wickle-mcp/tests/stdio.rs::multipage_snapshot_keeps_schema_and_oversized_input_never_dispatches`

### 예측 → 결함 → 복구

잘못된 도구 제안을 네 번 준 뒤 유효한 입력을 한 번 보내고 실제 server 기록을 센다.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

수정용 모델 호출은 여러 번이어도 remote 업무 Tool은 한 번만 실행돼야 한다. source 로그의 존재만 기다리지 말고 실제 완료 내용을 확인해야 file 생성/write 경쟁을 피할 수 있다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 57 --dest ../wickle-answer-57
python3 "$COURSE/lab.py" compare 57 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/57-mcp-repair.patch"
git apply "$COURSE/solutions/57-mcp-repair.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/mcp.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
