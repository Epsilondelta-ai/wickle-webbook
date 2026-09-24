# 60장. Wickle 0.2.0 완성과 배포 검증

[목차](README.md) · [이전](59-host-migration.md) · [다음](assessment.md) · [버전별 변경 지도](changes-v0.2.md)

## 학습 목표와 출발점

59장의 검사를 마친 동일 실습 workspace에서 이어간다. 0.2.0을 완성했다는 것은 13개 crate와 Host가 같은 release 계약을 사용하며, 저장 migration과 배포 파일까지 검증했다는 뜻이다. Cargo 버전만 바꾸면 끝나는 작업이 아니다.

이번 장의 정확한 checkpoint는 `b1416772dd185a3b175db6e3099512d9006f48f5`이다. 37–59장은 최종 0.2.0으로 가는 중간 구현이며 package version이 아직 0.1.0일 수 있다. 마지막 60장에서 release metadata까지 완성한다. 이 장의 코드는 [전체 구현·검사](implementation/60-release-v02.md)와 [정답 patch](solutions/60-release-v02.patch)에 생략 없이 제공한다.

## Rust와 컴퓨터공학 연결

Rust Book의 [오류 처리](https://doc.rust-lang.org/book/ch09-00-error-handling.html), [trait·generic·lifetime](https://doc.rust-lang.org/book/ch10-00-generics.html), [테스트](https://doc.rust-lang.org/book/ch11-00-testing.html), [async](https://doc.rust-lang.org/book/ch17-00-async-await.html)를 필요할 때 다시 읽는다. 문법은 01–02장에서 익히고, 여기서는 누가 데이터를 소유하며 언제 저장·외부 호출·권한 검사를 하는지를 추적한다.

## 강의와 구현 순서

1. 모든 workspace crate와 내부 exact dependency를 0.2.0으로 맞추고 외부 lockfile 의존성 변경 범위를 확인한다.

2. README·설치 태그·CHANGELOG·migration의 대상 버전을 일치시킨다. 저장 JSON의 모든 v1 문자열을 v2로 일괄 치환하지 않는다.

3. 전체 workspace default-debug tests와 전체 extracted package 소비자를 실행하고 최종 참조 367파일과 비교한다.

4. terminal 기록 보존·active drain·backup·new admission upgrade·downgrade 제한을 학습용 DB에서 검사한다. provider live는 release 증거와 교재 재실행을 분리한다.

먼저 `python3 "$COURSE/lab.py" inspect 60`으로 변경 파일을 확인한다. 전체 코드를 한 번에 복사하기 전에 테스트의 input·expected outcome을 읽고, 자료형 → 순수 검증 → 상태/전송 경계 → 소비자 순서로 직접 작성한다. 실행 전 상태가 무엇이며 실패하면 어디까지 남는지 각 함수 옆에 적어 본다.

디스크가 부족한 환경의 정확한 test/clean 명령은 [최종 평가의 용량 제한 검사](assessment.md)에 있다.

## 실제 코드에서 경계 찾기

아래는 `Cargo.toml`의 checkpoint 1행부터 읽는 발췌다. **독립 실행용 전체 프로그램이 아니다.** 전체 파일과 import는 구현 문서에 있다.

```toml
[workspace]
members = ["crates/wickle", "crates/wickle-model-router", "crates/wickle-state-sqlite", "crates/wickle-adapter-runtime", "crates/wickle-model-openai", "crates/wickle-model-responses", "crates/wickle-model-azure-openai", "crates/wickle-model-anthropic", "crates/wickle-model-bedrock", "crates/wickle-model-gemini", "crates/wickle-model-vertex", "crates/wickle-model-xai", "crates/wickle-mcp"]
default-members = ["crates/wickle"]
resolver = "3"

[workspace.package]
version = "0.2.0"
license = "MIT"
edition = "2024"
rust-version = "1.85"
repository = "https://github.com/Epsilondelta-ai/wickle"

[workspace.dependencies]
getrandom = { version = "=0.3.4", default-features = false }
futures-util = { version = "=0.3.34", default-features = false, features = ["std", "async-await"] }
jsonschema = { version = "=0.56.0", default-features = false }
rusqlite = { version = "=0.40.2", default-features = false, features = ["bundled"] }
reqwest = { version = "=0.13.5", default-features = false, features = ["rustls"] }
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

코드의 구조를 다음 네 질문으로 설명한다.

- 인자가 원래 제출·저장된 값·현재 runtime 객체 중 무엇인가?
- 검증 실패가 발생하면 아직 시작하지 않은 외부 동작은 무엇인가?
- `Result`로 전달하는 오류와 저장된 outcome은 어떻게 다른가?
- 재호출하면 같은 record를 읽는가, 새 attempt를 만드는가?

## 설계 이유·패턴·장단점

Release engineering과 versioned contracts의 통합이다. 소스 태그·배포 checksum은 재현할 artifact의 identity를 확인하지만 계정별 모델 가용성이나 업무 성공을 보장하지 않는다. DB 백업 복원 역시 백업 이후 외부 write를 취소하지 않는다.

가장 단순한 대안과 비교한다. 현재 값을 매번 다시 읽는 방법은 코드가 짧지만 replay 의미가 바뀔 수 있고, 모든 데이터를 복제하면 재현은 쉬워도 저장·검증 비용이 증가한다. 이 장의 선택이 어떤 구체적 실패를 막는지 아래 실험으로 확인한다. 패턴 이름 자체를 완성 조건으로 삼지 않는다.

## 실습 검증

모든 패키지의 unit/integration/doctest를 기본 debug에서 순서대로 실행한다. 한꺼번에 모든 binary를 링크하는 `cargo test --workspace --locked`도 디스크가 충분하면 사용할 수 있다. 이번 제작 검증은 disk 한도로 패키지별 실행을 사용했다. 명령 사이에서 필요하면 해당 학습 workspace의 완료된 Cargo 산출물만 정리한다.

```sh
cargo test -p wickle --locked
cargo test -p wickle-adapter-runtime --locked
cargo test -p wickle-mcp --locked
cargo test -p wickle-model-anthropic --locked
cargo test -p wickle-model-azure-openai --locked
cargo test -p wickle-model-bedrock --locked
cargo test -p wickle-model-gemini --locked
cargo test -p wickle-model-openai --locked
cargo test -p wickle-model-responses --locked
cargo test -p wickle-model-router --locked
cargo test -p wickle-model-vertex --locked
cargo test -p wickle-model-xai --locked
cargo test -p wickle-state-sqlite --locked
```

또는 같은 검사를 helper로 실행한다.

```sh
python3 "$COURSE/lab.py" check 60 --work .
```

기대 결과는 실패 0과 종료 코드 0이다. 이름 필터를 잘못 써서 0개만 실행한 것을 성공으로 보지 않는다. default debug·기본 thread stack을 사용한다. 스택 결함을 숨길 수 있으므로 `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, 큰 `RUST_MIN_STACK`으로 이 검사를 대체하지 않는다. 빌드 용량을 줄이려면 `CARGO_INCREMENTAL=0`을 사용하고, 동작 검사가 끝난 작업용 target만 정리한다.

읽을 행동 테스트:

- 모든 crate의 unit/integration/doctest와 별도 패키지 소비자를 실행한다.

### 예측 → 결함 → 복구

library 0.2.0인데 Profile의 wickle.agent-profile.v1을 사용하면 오류인가? active 구 Run을 v2 default segment로 채우면 되는가?

먼저 예상 결과를 적고, 관련 테스트와 fixture를 읽어 실제 관찰 항목을 찾는다. 결함을 넣어 실패함을 확인하고 제거한 뒤 다시 성공시킨다. 핵심 검사 대상은 최종 문장뿐 아니라 callback·HTTP·executor 횟수, saved revision, receipt, scope, 원문 보존이다. fixture 호출 수를 실제 provider 요청 수라고 부르지 않는다.

<details>
<summary>해설</summary>

라이브러리·JSON 형식의 버전은 별개라 v1 표기는 여전히 유효할 수 있다. 두 번째는 안 된다. 필요한 과거 근거를 새 값으로 발명하지 말고 구 runtime drain 후 안전한 이관 절차를 적용한다.

</details>

## 막혔을 때 정답 비교

직접 쓴 파일을 덮어쓰지 않고 별도의 폴더에서 기준을 확인한다.

```sh
python3 "$COURSE/lab.py" snapshot 60 --dest ../wickle-answer-60
python3 "$COURSE/lab.py" compare 60 --work .
```

기존 폴더는 snapshot 도구가 거절한다. 다른 구현은 byte 비교가 달라도 행동이 맞을 수 있으므로 테스트와 설계 설명을 함께 평가한다. 전 단계의 정확한 정답에서 이어갈 때만 아래 patch를 적용한다. 직접 작성한 구현은 먼저 별도 보관하고 patch를 강제로 덮지 않는다.

```sh
git apply --check "$COURSE/solutions/60-release-v02.patch"
git apply "$COURSE/solutions/60-release-v02.patch"
```

## 설계·변경 근거와 다음 단계

- [0.2.0 최종 사용 계약](../reference/docs/releases.md): 최종 API와 제약을 확인한다. 중간 checkpoint와 final signature를 혼합하지 않는다.
- 기존 구현·검증 기록 (로컬 교재의 참고 기록): 초기 실패와 후속 수정까지 있는 작업 기록이다. 중간의 In progress 문구보다 마지막 완료·정정 기록을 읽는다.
- 설계 근거 지도 (로컬 교재 참고): 사용자 결정·활성 설계·태그 소스의 우선순위를 정리했다.

위 설명과 실제 저장/호출 경계를 자신의 말로 연결하고 검사에 통과하면 다음 장으로 진행한다. 기존 릴리스의 live 확인을 이번 로컬 실습의 live 성공으로 승계하지 않는다.
