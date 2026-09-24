# 0.2.0 교재 검증 결과

[목차](README.md) · 기초 버전의 이전 검증 (로컬 교재 참고)

기준일: 2026-09-24. 최종 소스: `v0.2.0` / `b1416772dd185a3b175db6e3099512d9006f48f5`. 환경: macOS arm64, Rust 1.98.1. 이번 검증은 원래 Wickle 작업 디렉터리를 변경하지 않고 별도 임시 폴더에서 수행했다.

## 소스·실습 재현

- 03–60장 58개 checkpoint를 정답 patch만으로 순차 복원했다. 각 단계의 모든 파일을 해당 Git commit의 archive와 독립적으로 비교했으며, 누락·추가 파일 없이 일치했다.
- 최종 참조는 367개 파일이며 0.1.0 참조는 313개 파일이다. 각 파일 hash와 별도 제공 reference의 내용도 일치한다.
- helper로 별도의 60번 snapshot을 다시 만들고 checksum 비교, 기존 폴더 덮어쓰기 거절, 사용자 메모 보존, 변경 파일 검출, 잘못된 chapter 거절, 손상 patch의 쓰기 전 거절을 확인했다.
- 37–59장의 지정 테스트를 각 실제 중간 소스에서 실행했다. 50–57장은 추가 `agent_contract` integration test도 각 중간 checkpoint에서 별도로 실행했다.
- 03–36장의 기존 테스트 전체를 이번에 다시 실행하지는 않았다. 해당 34개 단계는 이번 소스 재현 검사와 이전 0.1.0 교재 검증 기록을 구분한다.

## 최종 패키지별 자동 검사

전체 `cargo test --workspace --locked`의 최초 시도는 테스트 assertion이 아니라 링크 단계의 디스크 부족(`errno=28`)으로 중단됐다. 이후 **13개 패키지별 unit/integration/doctest 전체를 순서대로 실행**하고 완료한 작업용 build 산출물을 정리하여 모두 통과했다. 한꺼번에 workspace 명령이 성공했다고 기록하지 않는다.

모든 완료 검사에서 기본 debug와 기본 thread stack을 사용했다. `CARGO_INCREMENTAL=0`, `CARGO_BUILD_JOBS=2`로 빌드 자원만 제한했고, debug=0이나 큰 RUST_MIN_STACK으로 결과를 대체하지 않았다. 초기 profile 검사를 준비하다 설계 기록의 조건을 확인하여 debug=0 실행을 중단하고 기본 profile로 다시 시작했다.

| 패키지 | 명령 | 결과 |
| --- | --- | --- |
| wickle | `cargo test -p wickle --locked` | 통과 |
| wickle-adapter-runtime | `cargo test -p wickle-adapter-runtime --locked` | 통과 |
| wickle-mcp | `cargo test -p wickle-mcp --locked` | 통과 |
| wickle-model-anthropic | `cargo test -p wickle-model-anthropic --locked` | 통과 |
| wickle-model-azure-openai | `cargo test -p wickle-model-azure-openai --locked` | 통과 |
| wickle-model-bedrock | `cargo test -p wickle-model-bedrock --locked` | 통과 |
| wickle-model-gemini | `cargo test -p wickle-model-gemini --locked` | 통과 |
| wickle-model-openai | `cargo test -p wickle-model-openai --locked` | 통과 |
| wickle-model-responses | `cargo test -p wickle-model-responses --locked` | 통과 |
| wickle-model-router | `cargo test -p wickle-model-router --locked` | 통과 |
| wickle-model-vertex | `cargo test -p wickle-model-vertex --locked` | 통과 |
| wickle-model-xai | `cargo test -p wickle-model-xai --locked` | 통과 |
| wickle-state-sqlite | `cargo test -p wickle-state-sqlite --locked` | 통과 |

## 직접 소비자 실행

- `python3 scripts/check-package.py --allow-dirty` 전체 실행 통과. 13개 추출 라이브러리와 두 독립 업무 workspace를 확인했다.
- 전체 suite 안에서 agent·resume·tool_schema·canonical·execution_contract·recovery·diagnostic 관련 소비자와 각 provider/MCP 예제를 실행했다. 선택 consumer만으로 전체 통과를 대신하지 않았다.
- 새 `examples/contracts-v02` 실행 통과. 큰 정수 구별, 원문 roundtrip, missing/empty 정규화와 null 거절, 얕은 옵션 교체를 실제 public API로 검사했다.
- SQLite·MCP child 등 로컬 자원은 실제로 사용한다. 모델은 synthetic 또는 loopback HTTP fixture이므로 유료 provider API 검증이 아니다.

## 문서 검사와 한계

목차·강의·전체 구현·patch 58개 대응, 상대 링크, code fence, manifest hash를 검사했다. 동봉된 설계·변경 사본 44개를 코드·최종 문서와 연결하고 초기 실패/후속 정정을 함께 설명했다.

이번 작업에서 실제 provider live, Linux, Rust 1.85, 하드웨어 전원 차단을 새로 실행하지 않았다. 원 release의 CI/live 보고와 이번 교재 검증을 섞지 않는다. 초심자 대상 수업 실험 및 모든 자율 연습문제의 수동 수행도 별도다.

## 결과 파일

- checkpoint 결과 (로컬 검증 기록)
- 공급자·MCP 단계별 Agent 통합 검사 (로컬 검증 기록)
- 최종 13개 패키지 검사 (로컬 검증 기록)
- 전체 패키지 소비자 결과 (로컬 검증 기록)
- 새 계약 예제 결과 (로컬 검증 기록)
- helper 실패 경계 검사 (로컬 검증 기록)
- 참조 코드 비교 (로컬 검증 기록)
- 설계 원문 identity (로컬 검증 기록)

각 `chapter-*.log`, `package.log`, `contract-example.log`는 이번 실행 증거다. `workspace-disk-failure.log`는 최초 환경 실패 기록이다. 임시 실행 소스와 build cache는 검증 후 정리하고 제품 소스나 기존 사용자 파일은 변경하지 않았다.
