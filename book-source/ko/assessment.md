# Wickle 0.2.0 최종 구현 평가

[목차](README.md) · [60장](60-release-v02.md) · [이관 실습](migration-lab.md)

완성은 0.1.0 기초를 만든 뒤 새 계약·어댑터·저장 이관까지 연결하는 것이다. 아래 명령은 **60번 checkpoint의 실습 workspace 루트**에서 실행한다. 학습자가 만든 코드에 대한 검사이며 제작자가 수행한 [검증 기록](VALIDATION.md)과 구분한다.

## 1. 전체 자동 검사

```sh
export CARGO_INCREMENTAL=0
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo doc --workspace --no-deps --locked
python3 "$COURSE/lab.py" compare 60 --work .
```

기본 debug와 기본 thread stack을 유지한다. `debug=0` 또는 `RUST_MIN_STACK` 증가로 실제 stack 결함을 숨기지 않는다. 구조적으로 같은 결과를 내는 별도 구현은 byte 비교가 다를 수 있다. 그 경우 모든 계약 시험과 변경 이유를 기록한다. helper는 추가 학습 파일을 비교하지 않으므로 추가 파일의 적절성도 따로 본다.

최소 지원 버전을 추가 확인하려면 Rust 1.85.0 설치 후 `cargo +1.85.0 test --workspace --locked`를 별도로 수행한다. 현재 개발 toolchain 검사와 과거 release CI의 MSRV 성공을 동일한 재실행으로 기록하지 않는다.

## 디스크가 부족할 때의 동일 profile 검사

모든 test binary를 한꺼번에 보관할 공간이 부족하면 아래 명령을 사용한다. **각 package의 검사가 종료된 뒤 그 package의 build 산출물만 정리**한다. 소스·Cargo.lock·학습 메모는 지우지 않는다. 같은 workspace를 다른 터미널에서 동시에 빌드하지 않는 상태에서 실행한다.

```sh
cargo test -p wickle --locked && cargo clean -p wickle
cargo test -p wickle-adapter-runtime --locked && cargo clean -p wickle-adapter-runtime
cargo test -p wickle-mcp --locked && cargo clean -p wickle-mcp
cargo test -p wickle-model-anthropic --locked && cargo clean -p wickle-model-anthropic
cargo test -p wickle-model-azure-openai --locked && cargo clean -p wickle-model-azure-openai
cargo test -p wickle-model-bedrock --locked && cargo clean -p wickle-model-bedrock
cargo test -p wickle-model-gemini --locked && cargo clean -p wickle-model-gemini
cargo test -p wickle-model-openai --locked && cargo clean -p wickle-model-openai
cargo test -p wickle-model-responses --locked && cargo clean -p wickle-model-responses
cargo test -p wickle-model-router --locked && cargo clean -p wickle-model-router
cargo test -p wickle-model-vertex --locked && cargo clean -p wickle-model-vertex
cargo test -p wickle-model-xai --locked && cargo clean -p wickle-model-xai
cargo test -p wickle-state-sqlite --locked && cargo clean -p wickle-state-sqlite
```

각 줄이 실패하면 다음 줄로 넘어가지 말고 해당 오류부터 확인한다. 기본 debug와 stack은 그대로 유지된다. `lab.py check 60`은 정리를 자동 수행하지 않으므로 디스크가 제한된 환경에서는 위 직접 명령을 사용한다. 이 방식의 실행 결과를 “cargo test --workspace 한 번 실행 성공”으로 바꾸어 기록하지 않는다.

## 2. 실제 공개 소비자 실행

```sh
python3 scripts/check-package.py --allow-dirty --consumer agent
python3 scripts/check-package.py --allow-dirty --consumer tool_schema
python3 scripts/check-package.py --allow-dirty --consumer resume
python3 scripts/check-package.py --allow-dirty
```

선택 실행은 빠른 피드백용이다. 최종 full suite는 13개 library package와 두 독립 업무 Host를 포함하며 include 의존성이 깨진 다른 소비자도 검사한다. 실제 provider API 호출은 하지 않는다. 모델은 synthetic/loopback fixture이며 SQLite와 로컬 MCP child 등은 실제 자원을 사용한다.

## 3. 필수 행동 기준

| 시나리오 | 확인할 근거 | 허용하지 않는 결과 |
| --- | --- | --- |
| 큰 JSON 숫자 두 값 | 원문·digest 차이 | 반올림으로 같은 요청이 됨 |
| 기본값 변경 뒤 중복 Start | 저장 제출 비교, resolver 0 추가 호출 | 최신 설정 때문에 과거 요청을 재해석 |
| 동시 다른 payload/같은 key | 승자 snapshot만 존재 | 다른 payload가 같은 요청으로 수락 |
| options override | shallow effective map·출처·schema revision | deep merge 또는 agent 옵션의 무조건 auxiliary 상속 |
| provider optional/null | codec roundtrip | 생략과 null 합침 |
| 잘못된 완전 Tool args | raw 보존·bounded repair | stream 전체 손상으로 원문 유실 또는 즉시 실행 |
| required default | 누락된 최상위 모델 field에 적용 | system ID나 explicit null에 default 주입 |
| 같은-input retry | 동일 PreparedStep, 새 physical 예약 | current compiler/source 재실행 |
| fallback 뒤 재시작 | 고정 target/revision과 단일 fallback charge | 다른 route로 이동·예산 중복 |
| 광고하지 않은 Tool 이름 | executor 0 | registry에 있으므로 실행 |
| 원천 권한 철회/Deleted | 요약·파생 history까지 검사 | summary로 자료 재노출 |
| local stop | Interrupted + 고정 AppState | 사용자 Cancel을 Pause로 변경 |
| callback timeout/panic | 안전 기본값·보호 진단 | 늦은 callback 성공으로 결과 덮어쓰기 |
| resume·command 경쟁 | acceptance/segment/lease 원자성 | 절반 상태·복수 소유자 |
| 과거 handle | 이전 Waiting/Interrupted outcome 유지 | latest outcome으로 교체 |
| idle Expire | 조회는 불변, 명령은 control segment | get_run만으로 상태 변경 |
| inspect_step | 저장 읽기·redaction·외부 callback 0 | diagnostic 조회가 다시 모델 실행 |
| 진단 권한 경쟁 | 최종 전체 context 집합 권한 | 첫 항목의 오래된 허용 재사용 |
| 외부 write 후 SIGKILL | Unknown/reconcile, 자동 재실행 없음 | crash를 NotApplied로 단정 |
| legacy DB | terminal 읽기 불변·active drain·원자 upgrade | 과거 actor/segment 발명 |
| 13개 crate와 package | 같은 release·독립 소비자 실행 | 다른 exact dependency·workspace 암묵 의존 |

각 행의 실제 테스트 이름과 관찰한 call count·revision·record를 적는다. prompt에 특정 문장이 들어 있는지 확인하는 것만으로 agent가 올바르게 수행한다고 평가하지 않는다.

## 4. 설계 구술 평가

1. canonical 계약을 provider subset으로 옮기면서도 원본 validation을 유지하는 이유는 무엇인가?
2. RequestSnapshot과 ModelConfiguration을 왜 같은 객체로 합치지 않는가?
3. command 소비·segment 생성·lease 획득 중 하나라도 분리 저장하면 어떤 crash 상태가 가능한가?
4. PreparedStep이 있으면서도 현재 ACL을 검사해야 하는 이유는 무엇인가?
5. app_state와 core status를 섞으면 callback이 어떤 안전 조건을 뒤집을 수 있는가?
6. 조회와 만료 확정, 명령 수락과 처리, 준비와 전송 증거는 왜 다른가?
7. metadata hash를 다시 계산한 위조 record도 구조적 관계 검사를 통과해야 하는 이유는 무엇인가?
8. depth limit만으로 schema compiler의 작업량을 제한할 수 없는 이유는 무엇인가?
9. Box::pin과 factory boxing, await와 cooperative yield의 차이는 무엇인가?
10. 이 설계를 exactly-once·순수 Event Sourcing·범용 Saga라고 단정하면 왜 부정확한가?

[아키텍처](02b-architecture.md)와 37–60장 내용을 자신의 구체적인 실패 예제로 설명한다. 단순 패턴 이름보다 불변 조건과 tradeoff가 중요하다.

## 5. 완료 제출물

직접 구현한 소스, 자동 검사 결과, 전체 package 소비자 결과, [migration lab](migration-lab.md)의 설명, 실패를 주입했다가 고친 기록을 함께 보관한다. actual provider live·Linux·MSRV·전원 손실을 실행하지 않았다면 미실행이라고 적는다. 저장 자료 복원과 과거 외부 효과 rollback은 서로 다르다는 점을 반드시 설명한다.
