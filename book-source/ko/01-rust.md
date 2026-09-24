# 01장. Wickle을 읽기 위한 Rust 기초

Rust·에이전트의 기초를 먼저 익히는 장이다. 최종 구현 목표는 0.2.0이며 03–36장의 기초 구현과 37–60장의 개선 과정을 차례로 진행한다.

[목차](README.md) · [이전](00-start.md) · [다음](02-async.md)

주 교재는 [The Rust Programming Language](https://doc.rust-lang.org/book/)다. 여기서는 Book을 복제하지 않고 Wickle 코드에서 실제로 만나는 문법을 작은 프로그램에 연결한다. Book 연습을 병행하고 다음 문법을 설명할 수 있을 때 구현 장으로 넘어간다.

## 1. 값, 소유권, 빌림

[Book 3장](https://doc.rust-lang.org/book/ch03-00-common-programming-concepts.html)과 [4장](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html)을 읽는다. 아래는 실행 가능한 [ownership.rs](examples/ownership.rs)의 전체 코드다.

```rust
use std::sync::Arc;

fn describe(request: &str) {
    println!("request: {request}");
}

fn main() {
    let request = String::from("report");
    describe(&request);
    let shared = Arc::new(request);
    let observer = Arc::clone(&shared);
    println!("shared owners: {}", Arc::strong_count(&shared));
    assert_eq!(observer.as_str(), "report");
}
```

`let`은 기본적으로 변경할 수 없는 binding을 만든다. 문자열을 바꾸려면 `let mut`이 필요하다. `String`은 소유한 문자열이고 `&str`은 문자열을 빌려 보는 slice다. `describe(&request)`는 값의 소유권을 옮기지 않으므로 호출 뒤에도 request를 사용할 수 있다.

`Arc::new(request)`는 request의 소유권을 옮긴다. 그 다음 줄에 `println!("{request}")`를 추가하면 moved value 오류가 나야 한다. 이 오류를 확인하고 해당 줄을 제거한다. `Arc::clone`은 내부 문자열을 복사하지 않고 공유 소유자의 수를 늘린다. 공유한다고 내부 값을 마음대로 바꿀 수 있는 것은 아니다. Wickle은 `Arc<dyn StateStore>` 같은 형태로 여러 driver/handle이 같은 서비스를 소유하게 한다.

`&mut T`는 독점적으로 변경할 수 있는 빌림이다. 동일 값에 대해 변경 가능한 빌림과 겹치는 다른 접근을 무제한 허용하지 않기 때문에 C/C++의 dangling pointer와 data race 일부를 컴파일 단계에서 막는다. 컴파일러를 달래기 위해 무조건 clone하기보다, 함수를 값 소비·읽기 빌림·수정 빌림 중 무엇으로 만들지 먼저 결정한다.

## 2. struct, enum, match, Result

[Book 5장](https://doc.rust-lang.org/book/ch05-00-structs.html), [6장](https://doc.rust-lang.org/book/ch06-00-enums.html), [9장](https://doc.rust-lang.org/book/ch09-00-error-handling.html)을 읽고 다음 예제를 실행한다.

```sh
rustc +1.98.1 --edition=2024 "$COURSE/examples/contracts.rs" -o /tmp/wickle-contracts
/tmp/wickle-contracts
```

기대 출력은 `request-1: unknown is not safe to retry`다. 전체 구현은 [contracts.rs](examples/contracts.rs)에 있다.

- `struct RequestId(String)`는 tuple struct로 문자열을 감싼다. 생성자를 통해 빈 값 검사를 한 곳에 모은다.
- `impl RequestId`는 타입의 메서드/연관 함수를 정의한다. `Self`는 현재 타입이다.
- `Result<T, E>`는 `Ok(T)` 또는 `Err(E)`다. `?`는 성공값을 꺼내고 실패면 현재 함수에서 오류를 전달한다.
- `Option<T>`는 `Some(T)` 또는 `None`이다. 실패라는 의미의 Result와 다르다. 조회 결과가 없다는 정상 상황에는 Option이 적합하다.
- enum variant는 추가 데이터를 가질 수 있다. `Applied { receipt }`와 `Unknown`을 별개의 경우로 표현한다.
- `matches!`는 특정 패턴 일치 여부를 bool로 돌려주는 macro다. `match`는 분기별 결과를 만드는 데 사용한다.
- `#[derive(Debug, PartialEq)]`는 출력과 비교 구현을 컴파일러가 생성하게 한다. `!`가 붙은 println/assert_eq는 macro 호출이다.

학습 예제의 `retry_is_safe`는 효과 관점 하나만 설명한다. 실제 Wickle은 NotApplied라는 사실만으로 자동 재시도를 하지 않는다. 현재 정책, 명시적 retry 계약, 예약, 저장 상태도 필요하다. 예제를 완제품 정책으로 사용하면 안 된다.

## 3. 컬렉션과 JSON

[Book 8장](https://doc.rust-lang.org/book/ch08-00-common-collections.html)을 읽는다. `Vec<T>`는 순서가 있는 가변 배열이다. 도구 호출 순서는 의미가 있어 Vec에 보존한다. `BTreeMap<K,V>`는 정렬된 키를 제공하고 `BTreeSet<T>`는 중복 없는 정렬된 집합이다. 정렬은 digest의 재현성을 설명할 때 중요하지만 저장소에서 모든 순서를 마음대로 정렬하면 안 된다.

`serde_json::Value`는 임의 JSON 값을 표현한다. 이를 실제 `AgentProfile`로 변환하면 필드와 variant를 타입으로 검사할 수 있다. `serde` derive와 attribute는 저장 표현을 정의한다. `#[serde(deny_unknown_fields)]`는 오타 난 필드를 조용히 무시하지 않게 한다. 외부 crate의 derive는 Book의 derive 개념을 라이브러리가 확장한 것이며 serde 세부 동작은 교재 04장의 실제 코드와 테스트가 근거다.

byte와 문자 개수도 다르다. `"한".len()`은 UTF-8 byte 수 3이다. `&text[..limit]`를 임의 byte 위치로 자르면 문자 경계가 아니어서 panic할 수 있다. Artifact preview 실습에서 이 차이를 직접 검사한다.

## 4. trait, generic, lifetime

[Book 10장](https://doc.rust-lang.org/book/ch10-00-generics.html)을 읽는다. trait은 어떤 메서드를 제공해야 하는지 정하는 계약이다. `impl StateStore for MemoryStateStore`는 메모리 저장소가 그 계약을 충족하도록 구현한다.

`fn f<T: Trait>(value: T)`는 generic으로 구체적인 T에 맞춰 컴파일된다. `&dyn Trait`는 runtime에 실제 구현의 메서드를 찾아 호출하는 trait object다. Wickle Host는 `Arc<dyn PolicyPort>`처럼 같은 타입 위치에 서로 다른 정책 구현을 주입한다. 이는 정적 dispatch보다 간접 호출 비용이 있지만 I/O 중심 Port에서는 교체·격리 이점이 크다. 모든 trait이 object-safe/dyn-compatible한 것은 아니며 02장의 boxed Future 반환도 이와 관계가 있다.

`'a`는 참조들 사이의 유효 기간 관계를 표현한다. 메모리 수명을 늘리는 명령이 아니다. `fn get<'a>(&'a self) -> &'a T`는 반환 참조가 self에서 빌린 것보다 오래 살아남지 못한다는 관계다. 오류를 피하려고 모든 참조에 `'static`을 붙이는 것은 해결이 아니다. 소유한 값이나 Arc로 task 경계를 넘길지 결정해야 한다.

## 5. 모듈, 공개 API, 테스트

[Book 7장](https://doc.rust-lang.org/book/ch07-00-managing-growing-projects-with-packages-crates-and-modules.html), [11장](https://doc.rust-lang.org/book/ch11-00-testing.html)을 읽는다. Wickle 내부의 `mod policy`는 외부에서 `wickle::policy`를 공개하는 것과 다르다. `pub use policy::PolicyGate`는 외부 사용 경로를 `wickle::PolicyGate`로 만든다. 내부 파일 배치를 바꾸어도 이 공개 경로를 유지할 수 있다.

`src` 안의 unit test는 내부를 직접 검사할 수 있고 `tests/*.rs`는 라이브러리 사용자처럼 공개 API를 호출한다. package consumer는 아예 다른 프로젝트에서 추출한 archive를 빌드한다. 각 검사가 발견하는 결함이 다르므로 하나를 통과했다고 모두 검증했다고 말하지 않는다.

## 이해 확인

1. request를 Arc에 넣은 뒤 원래 이름으로 쓰지 못하는 이유는 무엇인가?
2. `Result<Option<T>, E>`에서 `Ok(None)`과 `Err(E)`는 어떻게 다른가?
3. 도구 결과의 실패와 외부 효과 Unknown을 왜 bool 하나로 합치면 안 되는가?
4. 모든 입력을 `serde_json::Value`로만 다룰 때 어떤 오류가 runtime까지 미뤄지는가?

해설: 1은 소유권 이동, 2는 정상적인 부재와 조회 실패의 차이, 3은 재실행 안전성 판단에 필요한 정보 손실, 4는 필드 이름·variant·자료형의 일관성 검사다. 각 답을 자신의 코드 예제로 보일 수 있어야 한다.
