# 02장. 비동기 Rust와 교체 가능한 Port

Rust·에이전트의 기초를 먼저 익히는 장이다. 최종 구현 목표는 0.2.0이며 03–36장의 기초 구현과 37–60장의 개선 과정을 차례로 진행한다.

[목차](README.md) · [이전](01-rust.md) · [다음: 아키텍처](02b-architecture.md)

Rust Book [15장 smart pointer](https://doc.rust-lang.org/book/ch15-00-smart-pointers.html), [16장 concurrency](https://doc.rust-lang.org/book/ch16-00-concurrency.html), [17장 async](https://doc.rust-lang.org/book/ch17-00-async-await.html), [18.2 trait object](https://doc.rust-lang.org/book/ch18-02-trait-objects.html)를 읽는다. Tokio의 구체적인 task/통신 API는 [공식 튜토리얼](https://tokio.rs/tokio/tutorial)을 보조 자료로 사용한다.

## 1. 동작부터 보기

```sh
cargo +1.98.1 run --manifest-path "$COURSE/examples/async-port/Cargo.toml" --locked
```

기대 출력:

```text
answer: report
dropping the task handle did not abort the worker
```

[전체 실행 코드](examples/async-port/src/main.rs)와 [Cargo.toml](examples/async-port/Cargo.toml)을 함께 읽는다. 외부 모델을 호출하지 않는 작은 학습 프로그램이며 Wickle을 축약 구현했다고 주장하지 않는다.

## 2. Future는 아직 진행 중인 스레드가 아니다

`async fn`을 호출하면 결과를 나중에 계산할 Future가 생긴다. runtime이 poll해야 진행한다. `.await`는 준비가 안 된 동안 현재 task가 다른 일을 실행할 기회를 runtime에 돌려주게 한다. 모든 `.await`가 별도 OS thread를 생성하는 것은 아니다.

`#[tokio::main]`은 main에서 runtime을 준비하고 비동기 본문을 실행하는 macro다. Wickle 라이브러리는 자체 runtime을 만들지 않고 Host가 준비한 Tokio 안에서 실행된다. 서버의 전체 실행 환경을 라이브러리가 몰래 결정하지 않도록 하기 위해서다.

CPU를 오래 쓰는 반복문이나 blocking DB 호출은 await를 붙인다는 이유만으로 비동기가 되지 않는다. SQLite adapter가 blocking pool을 쓰는 이유를 13장에서 확인한다.

## 3. boxed Future의 타입을 해부하기

```rust
// 읽기용 타입 선언. 전체 실행 파일은 위 링크에 있다.
type Reply<'a> = Pin<Box<dyn Future<Output = String> + Send + 'a>>;
```

| 조각 | 의미 | Wickle에서 필요한 이유 |
| --- | --- | --- |
| `Future<Output = String>` | 나중에 String을 산출하는 계산 | Port 호출이 I/O를 기다릴 수 있음 |
| `dyn Future` | 구체적인 Future 타입을 감춤 | 서로 다른 구현을 같은 trait 반환 타입으로 사용 |
| `Box` | heap에 둔 값을 소유 | 크기가 고정되지 않은 trait object를 포인터 뒤에 둠 |
| `Pin` | pinning 계약 아래 pointee 이동을 제한 | poll하는 Future가 요구하는 위치 안정성 표현 |
| `Send` | 소유권을 thread 경계로 옮길 수 있음 | runtime/task의 실행 제약을 만족 |
| `'a` | 빌린 self와 request보다 오래 살 수 없음 | request 참조를 사용하는 Future의 수명 관계 |

`Pin<Box<_>>`를 지역 변수 사이로 이동한다고 heap 안의 Future 자체가 이동하는 것은 아니다. 이 둘을 구분한다. unsafe pinning을 직접 작성할 필요는 없다. Wickle도 `Box::pin`과 safe wrapper를 사용한다.

`impl Model for ScriptedModel`에서 `Box::pin(async move { ... })`가 각 구현 고유의 Future를 공통 반환 타입으로 바꾼다. `move`는 캡처 값을 Future로 옮긴다. 캡처 값이 참조라면 참조 자체가 옮겨지는 것이지 참조 대상이 `'static`이 되는 것은 아니다.

## 4. Arc, Send, Sync, Mutex

`Arc<dyn Model>`은 runtime에 구체 구현을 선택할 수 있고 여러 실행자가 소유할 수 있다. Arc는 공유 소유권을 주지만 임의 내부 변경을 자동으로 안전하게 만들지는 않는다. `Sync`는 공유 참조를 thread 간에 공유할 수 있다는 계약이고 `Send`와 같은 뜻이 아니다.

Wickle의 메모리 StateStore는 잠금 안에서 메모리 변경을 원자적으로 수행한다. 잠금을 잡고 원격 요청을 await하면 다른 실행자도 장시간 막힐 수 있다. 먼저 필요한 값을 복사하고 lock을 해제한 뒤 I/O를 하되, 나중에 저장할 때 CAS로 이전 읽기가 여전히 유효한지 확인하는 것이 전형적인 흐름이다. 잠금을 해제했으니 race가 사라졌다고 가정하면 안 된다.

## 5. 실행 소유권과 관찰 소유권

예제에서 spawn한 task의 JoinHandle을 drop해도 worker는 계속 진행하고 oneshot으로 결과를 보낸다. 따라서 `drop(handle)`과 명시적 abort는 다르다. Wickle의 RunHandle도 단순 화면 관찰자의 수명에 업무 실행이 묶이지 않도록 설계한다.

반대로 Future를 더 이상 poll하지 않는 것은 해당 비동기 계산을 중단할 수 있다. 이미 외부 서비스로 보내진 결제가 되돌려졌음을 의미하지는 않는다. 취소는 로컬 제어 흐름의 사건이고 외부 효과 확정은 별도의 저장·조회 계약이다. 16·24장에서 Unknown을 보존하는 이유다.

## 6. Stream과 backpressure

Future가 결과 하나라면 Stream은 여러 값을 시간에 따라 반환한다. Wickle ModelPort는 text/tool/usage/finish event의 Stream을 반환한다. observer가 느리다고 모델 실행 소유권을 observer에게 넘기지 않는다. durable event를 저장하고 cursor로 읽으면 재접속할 수 있다. 텍스트 delta 같은 임시 관찰값과 확정 이벤트의 보장 수준도 다르다.

## 실습과 해설

예제의 `sender.send` 전에 oneshot 하나를 더 두어 worker 진행을 제어해 보라. handle을 drop한 뒤 gate를 열면 여전히 결과가 와야 한다. 다음에는 gate를 열기 전에 `handle.abort()`를 호출하고 결과 수신 실패를 확인하라. 이 둘의 차이를 코드로 설명한다. 이 연습에서 sleep 길이로 스케줄링을 추측하지 않고 명시적인 channel로 순서를 만든다.

추가 질문: 이 예제를 그대로 Wickle Run driver로 쓸 수 있을까? 아니다. 아직 admission 중복 억제, lease, 예산, durable result, 권한, 복구가 없다. 비동기 실행 기법은 엔진의 한 부품일 뿐이다.
