# 56장. xAI 인코딩 설명과 계약 버전 보존

[목차](README.md) · [이전](55-vertex-schema.md) · [다음](57-mcp-repair.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

55장의 검사를 마친 동일 실습 workspace에서 이어간다. identity codec만 쓰는데 presence envelope 사용법까지 prompt에 설명하면 모델이 필요 없는 포장을 할 수 있다. 설명 문자열도 실제 모델 입력의 일부이므로 버전과 digest에 포함된다.

이번 장의 정확한 checkpoint는 `7d9c034a23ddb41fa397433f61ff0c8e5d4d72a2`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/56-xai-contract.md)와 [정답 patch](solutions/56-xai-contract.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. xAI의 native optional/null/open-object 의미를 유지하고 표현이 어려운 field만 JsonText로 복원한다.

2. 공통 constraint 생성기가 실제 사용 중인 encoding의 설명만 내도록 계약 v2를 만든다.

3. 저장된 v1은 당시 설명·wire schema·codec·digest 그대로 복원한다. 새 설명으로 자동 재컴파일하지 않는다.

4. schema graph depth/node/byte 예산과 raw invalid replay, core retry/repair의 물리 호출 수를 검사한다.

먼저 `python3 "$COURSE/lab.py" inspect 56`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

## 실제 코드에서 경계 찾기

아래는 `crates/wickle-model-xai/src/schema.rs`의 checkpoint 8행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```rust
pub struct XaiToolSchemaCompiler;
impl ProviderToolSchemaCompiler for XaiToolSchemaCompiler {
    fn reference(&self) -> VersionedRef {
        VersionedRef {
            id: Id::new("wickle-xai-tool-schema").expect("constant"),
            version: Id::new("1").expect("constant"),
        }
    }
    fn compile(
        &self,
        tool: &ModelTool,
        target: &ProviderToolTarget,
    ) -> Result<ProviderToolProjection, ContractError> {
        if target.provider.as_str() != "xai"
            || target.api_contract.operation.as_str() != "responses"
            || target.api_contract.version.as_str() != "v1"
        {
            return Err(crate::error(
                ErrorCode::ModelCapabilityUnsupported,
                "schema_target",
            ));
        }
        let root = &tool.model_input_schema;
        let properties = root
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                crate::error(ErrorCode::InvalidToolInputContract, "schema_properties")
            })?;
        let mut budget = Budget {
            nodes: 1024,
            bytes: 32 * 1024,
        };
        let mut projected = Map::new();
```

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

프로토콜 버전은 자료 구조뿐 아니라 모델에게 전달하는 설명의 의미도 포함한다. 사용자 경험상의 혼동을 고쳐도 과거 실행의 재현성을 깨뜨리지 않는 versioned interpreter다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

```sh
cargo test -p wickle-model-xai --test schema --locked
cargo test -p wickle-model-xai --test responses --locked
cargo test -p wickle-model-xai --test agent_contract --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 56 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- `crates/wickle-model-xai/tests/schema.rs::target_mismatch_and_non_disableable_reasoning_fail_before_http`
- `crates/wickle-model-xai/tests/schema.rs::compact_branching_references_exhaust_a_shared_budget_and_fall_back_without_expanding`
- `crates/wickle-model-xai/tests/schema.rs::recursive_shapes_and_conflicting_reference_siblings_roundtrip_without_overwriting_rules`
- `crates/wickle-model-xai/tests/responses.rs::metadata_cancellation_scope_and_size_limits_are_independent_of_inference`
- `crates/wickle-model-xai/tests/responses.rs::reasoning_exceptions_never_allow_invalid_executable_identities`
- `crates/wickle-model-xai/tests/responses.rs::two_documented_release_ids_coexist_without_replacing_connection_state`

### 예측 → 결함 → 복구

같은 canonical tool로 v1 저장 계약과 v2 신규 계약을 비교해 wire와 digest가 반드시 모두 달라지는지 예측하라.

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

wire와 실행 복원 동작은 같을 수 있지만 설명이 바뀌면 계약 digest는 달라진다. 한 번의 live 실패/성공만으로 설명 변경의 인과관계 전체를 증명했다고 주장하지 않는다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 56 --dest ../wickle-answer-56
python3 "$COURSE/lab.py" compare 56 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/56-xai-contract.patch"
git apply "$COURSE/solutions/56-xai-contract.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/xai.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
