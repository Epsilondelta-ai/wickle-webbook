# 04장. Rust 타입으로 실행 계약 표현하기

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 새 JSON text encoder는 숫자 lexeme를 보존하며 과거 sorted-json-v1 digest와 구분한다. 새 저장 기록을 추가해도 Profile의 v1 schema ID를 일괄 변경하지 않는다.

이어지는 구현: [38장](38-canonical-json.md) · [39장](39-execution-contracts.md).

[목차](README.md) · [이전 장](03-workspace.md) · [다음 장](05-policy.md)

## 이번 장의 출발점과 결과

03장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **Rust 타입으로 실행 계약 표현하기**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/04-contracts.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/04-contracts.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch06-00-enums.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

실행 상태를 문자열과 bool 여러 개로 표현하면 finished=true와 waiting=true 같은 모순이 생긴다. enum은 가능한 경우를 나열하고 각 경우가 필요한 데이터를 함께 보관한다. 새 variant를 추가했을 때 match가 빠진 분기를 알려주는 것도 이점이다. Id(String)는 문자열을 감싼 newtype이다. 빈 식별자를 생성 시점에 막되 UUID나 날짜라고 추정하지 않는다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 04
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. error.rs와 serialization.rs부터 작성한다. ContractError는 오류 코드와 경로를 전달하고 Id::new는 입력을 검증한다. serde의 Serialize/Deserialize는 저장 형식 변환을 생성한다.

2. profile.rs, context.rs, message.rs, model.rs, run.rs의 데이터 타입을 추가한다. 직렬화하는 데이터와 소켓·Future·credential 같은 런타임 객체를 분리한다.

3. StrictJson visitor에서 키 중복을 검사한다. 일반 Map으로 먼저 파싱하면 중복이 덮어써져 사라지므로 그 뒤에는 검출할 수 없다. canonical digest는 객체 키 정렬 후 해시하되 배열 순서를 보존한다.

4. ProfileValidator와 resolver를 구현하고 정확한 버전·scope·구성 스키마를 검증한다. decode 성공, 구성 유효성, 현재 권한 확인은 서로 다른 검사다.

## 실제 코드 읽기

`crates/wickle/src/serialization.rs`의 이 단계 15–46행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/04-contracts.md)에 있다.

```rust
pub struct Id(String);

impl Id {
    /// Validate an identifier without normalizing its spelling.
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ContractError::new(ErrorCode::InvalidContract, "identifier"));
        }
        Ok(Self(value))
    }

    /// Return the original identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Id {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Id> for String {
    fn from(value: Id) -> Self {
        value.0
    }
}
impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Newtype과 데이터 중심 계약을 적용한다. 값 객체처럼 비교·해시할 수 있는 Id와 versioned reference가 경계의 혼동을 줄인다. 단, Id 하나가 모든 종류의 식별자를 감싸므로 tenant ID와 tool ID의 컴파일 타임 혼동까지 막지는 않는다. 별도 newtype을 늘리면 더 강한 타입 안전성을 얻지만 변환 코드와 API가 늘어난다. 엄격한 역직렬화는 조용한 설정 오타를 막는 대신 새 필드와의 전방 호환성을 제한한다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle --test contracts --locked
python3 "$COURSE/lab.py" check 04 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle/tests/contracts.rs` → `digest_matches_independent_sha256_vectors_and_sorts_nested_objects`
- `crates/wickle/tests/contracts.rs` → `ambiguous_and_non_json_input_is_rejected_instead_of_being_normalized`

### 결함을 주입하는 연습

동일 JSON 객체의 키 순서만 바꾸면 digest는 어떻게 되는가? 배열 순서, 숫자 1과 1.0은 어떤가? 중복 키가 있는 Profile을 입력하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

객체 키 순서만 다른 값은 동일하다. 배열 순서와 숫자 표현 차이는 보존될 수 있다. 구현은 RFC 8785라고 주장하지 않는다. 중복 키는 InvalidJson으로 거절되어야 하며 마지막 값으로 정상 처리하면 잘못된 구현이다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 04 --dest ../wickle-answer-04
python3 "$COURSE/lab.py" compare 04 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/04-contracts.patch"
git apply "$COURSE/solutions/04-contracts.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `4c63cf3e202303e5b415c52c505f1f44d491053c`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `CONTRIBUTING.md`
- `Cargo.lock`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle/Cargo.toml`
- `crates/wickle/src/context.rs`
- `crates/wickle/src/error.rs`
- `crates/wickle/src/lib.rs`
- `crates/wickle/src/message.rs`
- `crates/wickle/src/model.rs`
- `crates/wickle/src/profile.rs`
- `crates/wickle/src/resolution.rs`
- `crates/wickle/src/run.rs`
- `crates/wickle/src/serialization.rs`
- `crates/wickle/tests/contracts.rs`
- `docs/contracts.md`
- `scripts/check-package.py`
- `tests/support/consumer.rs`

</details>
