# 34장. 모델 버전별 지원 근거 검증

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 릴리스별 contract·live 증거를 분리한다. v0.1 통과를 바뀐 schema/compiler/옵션 경로의 검증으로 승계하지 않는다.

이어지는 구현: [50장](50-openai-schema.md) · [51장](51-azure-schema.md) · [52장](52-anthropic-repair.md) · [53장](53-bedrock-repair.md) · [54장](54-gemini-schema.md) · [55장](55-vertex-schema.md) · [56장](56-xai-contract.md) · [60장](60-release-v02.md).

[목차](README.md) · [이전 장](33-events.md) · [다음 장](35-integration.md)

## 이번 장의 출발점과 결과

33장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **모델 버전별 지원 근거 검증**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/34-evidence.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/34-evidence.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch11-01-writing-tests.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

테스트 근거도 버전이 있는 데이터다. planned는 구성 계획, contract_tested는 해당 계약 검사, live_verified는 해당 실제 서비스 검증이다. 한 모델/지역/API 조합의 근거를 다른 조합에 재사용하면 지원 행렬이 거짓말을 하게 된다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 34
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. provider namespace별 두 release를 한 catalog에 등록하는 consumer를 작성한다.

2. definition·target·API·capability가 달라지면 evidence digest도 달라지는지 검사한다.

3. local HTTP fixture의 요청 모델 ID와 provider가 보고한 revision을 각각 확인한다.

4. 문서의 support matrix에서 synthetic/contract/live를 구분하고 unknown 값을 그대로 기록한다. 현재 판매·가용 모델 목록으로 release fixture를 해석하지 않는다.

## 실제 코드 읽기

`tests/support/version_matrix_consumer.rs`의 이 단계 1–32행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/34-evidence.md)에 있다.

```rust
// Synthetic catalog evidence and capabilities; no provider availability is claimed.
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use wickle::*;
use wickle_model_router::ImmutableModelCatalog;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}

fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}

fn definition(provider: &str, version: &str, wire: &str, features: &[&str]) -> ModelDefinition {
    ModelDefinition {
        model_key: id("example-model"),
        family: id("example-family"),
        provider: id(provider),
        model_id: id(wire),
        model_version: id(version),
        version_semantics: VersionSemantics::Pinned,
        lifecycle: ModelLifecycle::Active,
        capabilities: ModelCapabilities {
            revision: id(version),
            features: features.iter().map(|value| id(value)).collect(),
            options_schema: json!({
                "type":"object", "properties":{"fixture_option":{"const":version}}, "additionalProperties":false
            }),
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Evidence-based capability registry는 플러그인 확장의 신뢰 경계를 명시한다. 테스트 통과를 문자열 label로만 보관하면 오래된 결과가 새 코드에 전이되기 쉽다. digest 연결은 불일치를 잡지만 외부 보고서 자체의 진실성을 인증하지 않는다. Host의 검증 프로세스가 여전히 신뢰 기반이다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-router --test catalog --locked
python3 "$COURSE/lab.py" check 34 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-anthropic/tests/messages.rs` → `thinking_before_text_and_cumulative_usage_are_not_misread_as_visible_output`
- `crates/wickle-model-anthropic/tests/messages.rs` → `signed_empty_thinking_and_tool_inputs_are_replayed_once_with_results`
- `crates/wickle-model-azure-openai/tests/responses.rs` → `deployment_mapping_preserves_model_identity_and_refreshes_entra_per_request`
- `crates/wickle-model-azure-openai/tests/responses.rs` → `wrong_scope_route_api_and_credentials_never_make_an_http_request`

### 결함을 주입하는 연습

target region이나 capability schema만 바꾼 binding에 과거 live evidence를 붙여 등록하라. local fixture 통과만으로 live_verified로 올려 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

정확한 계약 근거가 없으면 승격을 거절해야 한다. local 서버는 codec과 경계를 검사하지만 실제 모델의 가용성·권한·답변 품질을 검사하지 않는다. 테스트 이름에 live가 포함되었다고 live 검증인 것도 아니다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 34 --dest ../wickle-answer-34
python3 "$COURSE/lab.py" compare 34 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/34-evidence.patch"
git apply "$COURSE/solutions/34-evidence.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `5a35b0e4cf484463005bb6a7a7eb4b396af3fc0f`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

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
- `crates/wickle-model-anthropic/tests/messages.rs`
- `crates/wickle-model-azure-openai/tests/responses.rs`
- `crates/wickle-model-bedrock/tests/bedrock.rs`
- `crates/wickle-model-gemini/src/codec.rs`
- `crates/wickle-model-gemini/tests/generate.rs`
- `crates/wickle-model-openai/tests/responses.rs`
- `crates/wickle-model-vertex/tests/vertex.rs`
- `crates/wickle-model-xai/tests/responses.rs`
- `docs/model-providers.md`
- `docs/model-support.md`
- `docs/validation/openai-live-smoke.json`
- `tests/support/version_matrix_consumer.rs`

</details>
