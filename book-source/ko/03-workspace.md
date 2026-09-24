# 03장. Cargo workspace와 첫 라이브러리

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

[목차](README.md) · [이전 장](02b-architecture.md) · [다음 장](04-contracts.md)

## 이번 장의 출발점과 결과

02장의 Rust 예제를 직접 실행한 빈 Wickle 실습 폴더에서 시작한다. 이번 장에서는 **Cargo workspace와 첫 라이브러리**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/03-workspace.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/03-workspace.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch07-00-managing-growing-projects-with-packages-crates-and-modules.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

crate는 Rust 컴파일 단위이고 package는 Cargo.toml이 정의하는 배포 단위다. workspace는 여러 package가 하나의 Cargo.lock과 공통 설정을 공유하게 한다. Wickle 코어를 lib.rs로 시작하면 CLI, 웹 서버, 데스크톱 프로그램이 같은 엔진을 사용할 수 있다. main.rs를 만들지 않는 것은 사용자 인터페이스를 Host에 남기기 위해서다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 03
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 빈 실습 디렉터리에 Cargo.toml과 crates/wickle/Cargo.toml을 만든다. 정답의 workspace members, resolver, edition, 의존성 버전을 그대로 사용한다. Cargo.lock도 구현 재현성의 일부다.

2. crates/wickle/src/lib.rs에 라이브러리 진입점을 만들고 테스트의 외부 import가 통하는지 확인한다. mod는 모듈을 선언하고 pub use는 외부에 보일 이름을 재노출한다.

3. rust-toolchain.toml, lint와 CI 설정을 추가한다. cargo check는 타입을 검사하고 cargo test는 실제 행동을 실행한다. 패키지 검사는 저장소 경로가 없는 별도 소비자까지 검사한다.

## 빈 폴더에서 만드는 첫 빌드

00장에서 만든 빈 `wickle-lab` 안에서 시작한다. 편집기로 아래 세 파일을 직접 작성한다. `cargo new`가 생성하는 기본 manifest 대신 workspace 상속 구조를 직접 만들어 본다.

```sh
mkdir -p crates/wickle/src
```

`Cargo.toml`:

```toml
[workspace]
members = ["crates/wickle"]
default-members = ["crates/wickle"]
resolver = "3"

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
repository = "https://github.com/Epsilondelta-ai/wickle"

[workspace.dependencies]
futures-util = { version = "=0.3.34", default-features = false, features = ["std", "async-await"] }
jsonschema = { version = "=0.56.0", default-features = false }
serde = { version = "=1.0.229", features = ["derive"] }
serde_json = "=1.0.151"
sha2 = { version = "=0.11.0", default-features = false }
thiserror = "=2.0.20"
tokio = { version = "=1.53.1", features = ["rt", "macros", "sync", "time"] }
tokio-util = { version = "=0.7.19", features = ["rt"] }

[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
```

`crates/wickle/Cargo.toml`:

```toml
[package]
name = "wickle"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
description = "An extensible agent engine for Rust applications"
publish = false
include = ["Cargo.toml", "src/**"]

[dependencies]
futures-util.workspace = true
jsonschema.workspace = true
serde.workspace = true
serde_json.workspace = true
sha2.workspace = true
thiserror.workspace = true
tokio.workspace = true
tokio-util.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "test-util"] }

[lints]
workspace = true
```

`crates/wickle/src/lib.rs`:

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! This crate currently contains the package foundation. Agent execution is
//! not implemented yet.
```

dependency의 정확한 버전 트리를 고정하는 Cargo.lock과 개발 compiler 설정은 기준 자료에서 가져온다. 이는 라이브러리 구현 코드를 복사하는 단계가 아니라 재현 환경을 맞추는 단계다.

```sh
python3 "$COURSE/lab.py" snapshot 03 --dest ../wickle-foundation-answer
cp ../wickle-foundation-answer/Cargo.lock .
cp ../wickle-foundation-answer/rust-toolchain.toml .
cargo check --workspace --locked
cargo test --workspace --locked
```

처음에는 실행할 테스트가 0개다. `Finished`와 종료 코드 0으로 workspace가 올바르게 연결되었음을 확인한다. 이것을 에이전트 기능 검증으로 기록하지 않는다. 나머지 `.gitignore`, CI, package 소비자 검사 파일과 문서는 이 장의 전체 patch에서 각 경로에 추가한다. 04장부터 실제 계약 테스트가 생긴다.

## 실제 코드 읽기

`crates/wickle/src/lib.rs`의 이 단계 1–4행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/03-workspace.md)에 있다.

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! This crate currently contains the package foundation. Agent execution is
//! not implemented yet.
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

모듈화와 정보 은닉의 출발점이다. 코어가 DB나 특정 모델 SDK를 직접 의존하면 그 제공자를 쓰지 않는 사용자도 해당 의존성과 변경 비용을 부담한다. Cargo crate 경계는 이를 빌드 그래프로 드러낸다. 반면 crate가 많아지면 버전 정합성과 패키징 검사가 필요하다. 단일 실행 파일이 목표인 작은 실험이라면 한 crate로 시작하는 쪽이 단순하다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test --workspace --locked
python3 "$COURSE/lab.py" check 03 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- workspace 테스트와 아래 독립 소비자 검사 결과를 확인한다.

### 결함을 주입하는 연습

core Cargo.toml에 SQLite 의존성을 넣으면 어떤 문제가 생기는가? cargo tree -p wickle --edges normal로 현재 직접·간접 의존성을 관찰하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

코어가 저장 방식까지 결정하면서 Port 교체의 이점이 줄어든다. 경계 검사도 허용되지 않은 코어 의존성을 잡아야 한다. 트리에 tokio가 있다는 것은 Wickle 0.1.0이 모든 런타임에 중립적이라는 뜻은 아니라는 점도 확인한다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 03 --dest ../wickle-answer-03
python3 "$COURSE/lab.py" compare 03 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/03-workspace.patch"
git apply "$COURSE/solutions/03-workspace.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `67ba3362310c798b7565aa2125e402fb97a1af2d`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `.github/workflows/ci.yml`
- `.gitignore`
- `CONTRIBUTING.md`
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
- `assets/mascot/prompt.md`
- `assets/mascot/wickle.png`
- `crates/wickle/Cargo.toml`
- `crates/wickle/src/lib.rs`
- `rust-toolchain.toml`
- `scripts/check-package.py`
- `tests/support/consumer.rs`

</details>
