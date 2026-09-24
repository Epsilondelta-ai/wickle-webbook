# Rust Book 대응표

[목차](README.md) · [Rust 기초](01-rust.md) · [비동기 Rust](02-async.md)

Rust 개념의 주 참고서는 [The Rust Programming Language](https://doc.rust-lang.org/book/)다. 최신 온라인 목차 기준의 장 링크를 사용한다. Wickle의 라이브러리 API·버전·동작은 고정된 0.1.0 소스가 기준이며 Book이 Wickle의 설계를 정의하는 것은 아니다.

| Book | 먼저 익힐 개념 | 연결되는 Wickle 강의 |
| --- | --- | --- |
| [1장](https://doc.rust-lang.org/book/ch01-00-getting-started.html) | rustup, rustc, cargo | 00, 03 |
| [3장](https://doc.rust-lang.org/book/ch03-00-common-programming-concepts.html) | let/mut, 함수, 조건/반복 | 01, 모든 구현 장 |
| [4장](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html) | 소유권, move, 참조, slice | 01–02, 10, 21, 28 |
| [5장](https://doc.rust-lang.org/book/ch05-00-structs.html) | struct, impl, 메서드 | 04–07 |
| [6장](https://doc.rust-lang.org/book/ch06-00-enums.html) | enum, Option, match, if let | 04, 16–17, 24 |
| [7장](https://doc.rust-lang.org/book/ch07-00-managing-growing-projects-with-packages-crates-and-modules.html) | crate/module/pub use, 공개 경계 | 03, 19, 26 |
| [8장](https://doc.rust-lang.org/book/ch08-00-common-collections.html) | Vec, String, Map | 04, 08–12, 21, 29 |
| [9장](https://doc.rust-lang.org/book/ch09-00-error-handling.html) | Result, ?, panic의 경계 | 04–08, 24 |
| [10장](https://doc.rust-lang.org/book/ch10-00-generics.html) | generic, trait, lifetime | 02, 08, 14, 20, 22 |
| [11장](https://doc.rust-lang.org/book/ch11-00-testing.html) | unit/integration test, 실패를 검출하는 assertion | 모든 장, 27, 34–36 |
| [12장](https://doc.rust-lang.org/book/ch12-00-an-io-project.html) | 작은 프로그램 구성, I/O, 오류 전달 | Host consumer를 읽기 전 |
| [13장](https://doc.rust-lang.org/book/ch13-00-functional-features.html) | closure, iterator, 성능 | 05, 18, 35 |
| [14장](https://doc.rust-lang.org/book/ch14-00-more-about-cargo.html) | workspace, profiles, docs, packaging | 03, 35–36 |
| [15장](https://doc.rust-lang.org/book/ch15-00-smart-pointers.html) | Box, Drop, 공유 소유권의 기초 | 02, 19, 32 |
| [16장](https://doc.rust-lang.org/book/ch16-00-concurrency.html) | thread, channel, Mutex, Arc, Send/Sync | 02, 06–07, 13, 24 |
| [17장](https://doc.rust-lang.org/book/ch17-00-async-await.html) | Future, async/await, Stream, Pin | 02, 08, 15, 25–32 |
| [18장](https://doc.rust-lang.org/book/ch18-00-oop.html) | trait object와 상태 모델링 비교 | 02B, 14–19, 31 |
| [19장](https://doc.rust-lang.org/book/ch19-00-patterns.html) | 패턴 매칭과 분해 | 04, 16–17, parser 구현 |
| [20장](https://doc.rust-lang.org/book/ch20-00-advanced-features.html) | 고급 trait/type, macro 읽기 | 공개 alias·serde derive를 더 공부할 때 |
| [21장](https://doc.rust-lang.org/book/ch21-00-final-project-a-web-server.html) | 스레드풀·graceful shutdown 비교 | 필수 구현 후 19, 32와 비교 |

## 순서와 필수 범위

처음에는 1·3·4·5·6·9장을 읽고 01장의 독립 프로그램을 실행한다. 다음으로 7·8·10·11·15·16·17·18.2장을 필요에 맞춰 02–08장과 병행한다. 고급 macro 작성이나 unsafe Rust를 먼저 마스터할 필요는 없다. 이 프로젝트는 unsafe code를 금지한다.

`Arc`와 `Mutex`는 Book 16장의 공유 상태를, `Pin<Box<dyn Future...>>`는 17장의 async trait 설명을 참조한다. `Rc`와 `RefCell`의 원리를 공부하되 이를 여러 thread가 사용하는 Port에 그대로 대입하지 않는다. 일반 패턴 매칭과 GoF 디자인 패턴은 다른 의미의 “패턴”이다.

Book의 소규모 예제에서 학습한 문법을 Wickle의 장기 실행 상태·트랜잭션에 적용할 때는 새 계약이 필요하다. Rust의 메모리 안전성만으로 업무의 멱등성, 권한, 데이터 진실성, 원격 효과 원자성이 보장되지는 않는다.

## 온라인과 오프라인

`rustup doc --book`으로 설치한 toolchain의 Book을 열 수 있다. 온라인 Book은 갱신되므로 목차나 예제가 설치판과 다를 수 있다. 교재의 checkpoint 명령은 Cargo.lock과 rust-toolchain.toml을 따르고, Book 예제를 이유로 release dependency를 일괄 갱신하지 않는다.

## 0.2.0 구현과 Rust Book 연결

| 강의 | Book에서 다시 볼 개념 | 실제 적용 |
| --- | --- | --- |
| 38–39 | 6·9·10장 enum/Result/trait | missing/null, version, 잘못된 상태 조합 |
| 40–41 | 16장 공유 상태 | lock 범위·CAS·transaction과 actor |
| 42–46 | 8·10장 map·소유권·trait | shallow merge·pure compiler·고정 snapshot |
| 44·46 | 15·17장 Box/Pin/Future | 생성 frame 크기, cooperative scheduling |
| 47–49 | 13·17장 closure/async | 제한된 callback과 부작용 없는 converter |
| 50–57 | 4·8·19장 slice/문자열/pattern | raw bytes·JSON 경계·가역 codec |
| 58–60 | 11·14장 tests/package | subprocess failure·독립 소비자·배포 |

Book의 메모리 안전성과 외부 업무의 안전성은 다르다. borrow checker가 중복 결제·권한 철회·저장 ack 유실까지 자동 해결하지 않는다.
