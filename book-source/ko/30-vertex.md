# 30장. Vertex AI 프로젝트·지역·토큰 경계

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 공유 Gemini compiler·raw bytes 경로를 연결하되 OAuth/project/location의 독립 경계는 유지한다. 릴리스 후 gcloud OAuth live 기록은 별도 자료다.

이어지는 구현: [55장](55-vertex-schema.md).

[목차](README.md) · [이전 장](29-gemini.md) · [다음 장](31-xai.md)

## 이번 장의 출발점과 결과

29장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Vertex AI 프로젝트·지역·토큰 경계**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/30-vertex.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/30-vertex.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch10-02-traits.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

Vertex는 Gemini 모델 의미를 공유하면서도 project/location, resource path와 Host access token이라는 별도 연결 계약을 가진다. global이라는 target 문자열도 모든 지역·계정에서의 실제 가용성 증명이 아니다. credentials를 core Profile에 넣지 않고 Host의 token provider에 남긴다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 30
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. Vertex connection에 project/location과 API resource 경로를 고정한다.

2. Gemini 공통 codec을 사용하되 Vertex target과 API contract를 별도로 확인한다.

3. Host token callback을 deadline과 cancellation 아래 호출한다. 오류·timeout 이후 늦은 token 결과로 HTTP를 시작하지 않는다.

4. inspection에서 실제 버전 근거와 route 일치를 검사하고 foreign scope/target은 네트워크 진입 전에 거절한다.

## 실제 코드 읽기

`crates/wickle-model-vertex/src/auth.rs`의 이 단계 6–37행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/30-vertex.md)에 있다.

```rust
pub enum VertexAudience {
    /// Generate content.
    Inference,
    /// Inspect publisher metadata.
    Metadata,
}
/// Explicit authorization context. Loading ADC remains the Host's responsibility.
pub struct VertexTokenContext<'a> {
    /// Connection owner.
    pub scope: &'a Scope,
    /// Resource project.
    pub project: &'a str,
    /// Request location, including global or a multi-region.
    pub location: &'a str,
    /// Inference or metadata operation.
    pub audience: VertexAudience,
    /// Cancellation while refreshing credentials.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Deadline covering credential resolution and HTTP.
    pub deadline: tokio::time::Instant,
}
/// Access token and optional actual expiry, with redacted Debug output.
#[derive(Clone)]
pub struct VertexToken {
    pub(crate) value: String,
    pub(crate) expires_at: Option<SystemTime>,
}
impl VertexToken {
    /// Construct from credentials obtained by the Host's chosen ADC/OAuth library.
    pub fn new(
        value: impl Into<String>,
        expires_at: Option<SystemTime>,
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

합성 기반 재사용이다. Gemini codec을 상속해 서비스별 예외를 덮는 거대한 계층 대신, 같은 codec을 다른 연결 객체와 조합한다. 이 구조는 인증 교체가 쉬운 반면 객체 간 계약을 맞추는 검증 코드가 필요하다. token provider는 Dependency Injection으로 테스트할 수 있지만 provider 구현의 숨은 retry는 Host가 통제해야 한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-model-vertex --test vertex --locked
python3 "$COURSE/lab.py" check 30 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-model-vertex/tests/vertex.rs` → `endpoint_families_follow_their_documented_hostnames`
- `crates/wickle-model-vertex/tests/vertex.rs` → `complete_signed_tools_and_current_output_format_use_the_scoped_resource`

### 결함을 주입하는 연습

토큰 callback이 deadline을 넘기고 나중에 성공하도록 만든다. 그 후 HTTP 요청이 생기는지 검사한다. project만 다른 route로 같은 연결을 사용해 보라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

늦은 token 성공은 만료된 호출을 되살리지 않는다. 정확한 target 경계를 넘어서는 재사용도 거절되어야 한다. 캐시된 token의 유효성과 특정 자원에 대한 권한은 별개의 조건이다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 30 --dest ../wickle-answer-30
python3 "$COURSE/lab.py" compare 30 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/30-vertex.patch"
git apply "$COURSE/solutions/30-vertex.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `c9f5882e327782a8592bf3f26dc46d9f0ba775c4`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `Cargo.toml`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-model-gemini/src/codec.rs`
- `crates/wickle-model-gemini/src/lib.rs`
- `crates/wickle-model-gemini/src/model.rs`
- `crates/wickle-model-gemini/src/response.rs`
- `crates/wickle-model-vertex/Cargo.toml`
- `crates/wickle-model-vertex/src/auth.rs`
- `crates/wickle-model-vertex/src/connection.rs`
- `crates/wickle-model-vertex/src/inspection.rs`
- `crates/wickle-model-vertex/src/lib.rs`
- `crates/wickle-model-vertex/src/model.rs`
- `crates/wickle-model-vertex/tests/support/mod.rs`
- `crates/wickle-model-vertex/tests/vertex.rs`
- `docs/env/vertex-ai.md`
- `docs/gemini.md`
- `docs/vertex.md`
- `scripts/check-package.py`
- `tests/support/vertex_consumer.rs`

</details>
