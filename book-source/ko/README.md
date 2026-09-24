# Wickle 0.2.0을 직접 만드는 Rust·에이전트 강의

최종 기준: **v0.2.0 / b1416772dd185a3b175db6e3099512d9006f48f5**. Rust 입문 컴퓨터공학 전공생을 위한 한국어 구현 교재다. 주 언어 교재는 [The Rust Programming Language](https://doc.rust-lang.org/book/)이며 문법·구현·설계 패턴·실패 검증을 함께 배운다.

**처음 시작하면 [00장](00-start.md), 0.1.0 학습을 마쳤다면 [37장](37-compatibility.md)부터 진행한다.** 기존 0.1.0을 완성하는 03–36장에 실제 변경 이력의 37–60장을 이어 붙였다. 중간 API를 최종 API와 섞지 않도록 기존 장 앞에 변경 안내를 추가했다. 최종 목표는 60장까지의 0.2.0 전체 구현이다.

- 62개 강의 문서: 00–60장과 02B 아키텍처.
- 58개 구현 checkpoint, 58개 전체 구현 문서와 정답 patch.
- 최종 참조 소스 367개 파일, 13개 crate. 별도의 0.1.0 참조도 보존한다.
- 기존 테스트 기록과 이번 재실행 검증은 [검증 기록](VALIDATION.md)에서 구분한다.

## 0.1.0 학습자를 위한 핵심 순서

1. 37–41장: 원문 JSON·제출 identity·저장 transaction을 갱신한다.
2. 42–46장: 옵션·provider schema·인자 수정·문맥 출처·PreparedStep을 연결한다.
3. 47–49장: 업무 상태와 중단·재개·진단의 경계를 구현한다.
4. 50–57장: 같은 계약을 일곱 provider와 MCP의 실제 전송 경로에 적용한다.
5. 58–60장: 장애·경쟁·구 데이터 이관과 독립 소비자로 최종 구현을 검증한다.

각 단계의 코드는 그 당시의 정확한 commit을 기준으로 한다. 0.2.0 최종 API만 빠르게 확인하려면 [Host 실행](walkthrough.md)과 [최종 reference 코드](../reference/crates/wickle/src/lib.rs)를 먼저 읽되, 직접 구현 과정은 위 순서를 따른다.

## 먼저 사용할 자료

| 문서 | 목적 |
| --- | --- |
| [변경 지도](changes-v0.2.md) | 기존 장의 규칙이 어디서 바뀌는지 확인 |
| [아키텍처·디자인 패턴](02b-architecture.md) | 경계·패턴의 이유와 비용, 0.2.0 보강 |
| 설계 근거 지도 (로컬 교재 참고) | 결정·설계·작업 기록과 실제 태그 구현 연결 |
| [0.2.0 Host 실행](walkthrough.md) | 중단·복구·옵션·진단을 직접 관찰 |
| [전체 Host 코드](host-example.md) | 생략 없는 실행 프로그램 읽기 |
| [이관 실습](migration-lab.md) | 소스 API와 저장 데이터 이관을 구별 |
| [최종 평가](assessment.md) | 실제 행동·실패·재개·배포 완료 조건 |
| [전체 소스 지도](code-atlas.md) | 각 파일의 최초/최종 구현 단계 |
| [Rust Book 대응표](rust-book-map.md) | 선행 개념과 적용 위치 |
| [용어집](glossary.md) | 도메인·비동기·저장·진단 용어 |

## 준비 강의

- [00 학습 방법과 개발 환경](00-start.md)
- [01 Rust 소유권·타입·오류·모듈](01-rust.md)
- [02 Future·Stream·Port와 실행 소유권](02-async.md)
- [02B 아키텍처와 디자인 패턴](02b-architecture.md)

## 기초 구현: 0.1.0 엔진

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 03 | [Cargo workspace와 첫 라이브러리](03-workspace.md) | [코드·테스트](implementation/03-workspace.md) |
| 04 | [Rust 타입으로 실행 계약 표현하기](04-contracts.md) | [코드·테스트](implementation/04-contracts.md) |
| 05 | [Scope와 현재 권한 검사](05-policy.md) | [코드·테스트](implementation/05-policy.md) |
| 06 | [원자적 저장소와 실행 소유권](06-state.md) | [코드·테스트](implementation/06-state.md) |
| 07 | [예산 예약·시간·취소](07-budget.md) | [코드·테스트](implementation/07-budget.md) |
| 08 | [모델 스트림과 물리 호출](08-model.md) | [코드·테스트](implementation/08-model.md) |
| 09 | [모델 입력과 시스템 입력 분리](09-schema.md) | [코드·테스트](implementation/09-schema.md) |
| 10 | [프롬프트 고정과 문맥 투영](10-context.md) | [코드·테스트](implementation/10-context.md) |
| 11 | [도구 실행 인자를 확정하고 저장하기](11-binding.md) | [코드·테스트](implementation/11-binding.md) |
| 12 | [모델 카탈로그·버전·옵션](12-catalog.md) | [코드·테스트](implementation/12-catalog.md) |
| 13 | [SQLite에 실행 상태 영속화하기](13-sqlite.md) | [코드·테스트](implementation/13-sqlite.md) |
| 14 | [정확한 모델 선택과 제한된 fallback](14-routing.md) | [코드·테스트](implementation/14-routing.md) |
| 15 | [Agent와 RunHandle로 첫 실행 완성](15-agent.md) | [코드·테스트](implementation/15-agent.md) |
| 16 | [직렬 Tool Loop와 외부 효과 원장](16-tools.md) | [코드·테스트](implementation/16-tools.md) |
| 17 | [승인·입력·외부 결과를 기다리고 재개](17-resume.md) | [코드·테스트](implementation/17-resume.md) |
| 18 | [Hook 변환과 결과 관찰](18-hooks.md) | [코드·테스트](implementation/18-hooks.md) |
| 19 | [어댑터 조립과 자원 수명](19-adapters.md) | [코드·테스트](implementation/19-adapters.md) |
| 20 | [검색과 메모리를 ContextSource로 연결](20-sources.md) | [코드·테스트](implementation/20-sources.md) |
| 21 | [Skill·Artifact·Evidence 구현](21-skills.md) | [코드·테스트](implementation/21-skills.md) |
| 22 | [문맥 선택·미리보기·압축](22-compaction.md) | [코드·테스트](implementation/22-compaction.md) |
| 23 | [출력 검증과 제한된 보완 루프](23-verification.md) | [코드·테스트](implementation/23-verification.md) |
| 24 | [프로세스 장애와 불확실한 효과 복구](24-recovery.md) | [코드·테스트](implementation/24-recovery.md) |

## 기초 구현: 어댑터·통합·첫 release

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 25 | [OpenAI Responses 어댑터](25-openai.md) | [코드·테스트](implementation/25-openai.md) |
| 26 | [Azure 배포·인증과 공통 Responses codec](26-azure.md) | [코드·테스트](implementation/26-azure.md) |
| 27 | [Anthropic Messages와 재현 가능한 복구 검사](27-anthropic.md) | [코드·테스트](implementation/27-anthropic.md) |
| 28 | [Bedrock 인증·바이너리 이벤트 프레임](28-bedrock.md) | [코드·테스트](implementation/28-bedrock.md) |
| 29 | [Gemini 스트림·도구 순서·서명](29-gemini.md) | [코드·테스트](implementation/29-gemini.md) |
| 30 | [Vertex AI 프로젝트·지역·토큰 경계](30-vertex.md) | [코드·테스트](implementation/30-vertex.md) |
| 31 | [xAI와 일곱 제공 경로 통합](31-xai.md) | [코드·테스트](implementation/31-xai.md) |
| 32 | [MCP stdio 도구와 프로세스 관리](32-mcp.md) | [코드·테스트](implementation/32-mcp.md) |
| 33 | [영속 이벤트와 외부 메모리 갱신](33-events.md) | [코드·테스트](implementation/33-events.md) |
| 34 | [모델 버전별 지원 근거 검증](34-evidence.md) | [코드·테스트](implementation/34-evidence.md) |
| 35 | [독립 Host 통합과 저장소 캐시 검증](35-integration.md) | [코드·테스트](implementation/35-integration.md) |
| 36 | [0.1.0 완성·패키징·최종 평가](36-release.md) | [코드·테스트](implementation/36-release.md) |

## 0.2.0: 계약·저장·모델 준비

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 37 | [호환성: 라이브러리 API와 저장 형식은 다르다](37-compatibility.md) | [코드·테스트](implementation/37-compatibility.md) |
| 38 | [숫자를 잃지 않는 JSON과 완료 정책](38-canonical-json.md) | [코드·테스트](implementation/38-canonical-json.md) |
| 39 | [제출 snapshot과 실행 segment를 타입으로 표현하기](39-execution-contracts.md) | [코드·테스트](implementation/39-execution-contracts.md) |
| 40 | [segment·lease·command를 한 트랜잭션으로 저장하기](40-atomic-store.md) | [코드·테스트](implementation/40-atomic-store.md) |
| 41 | [현재 설정보다 저장된 요청을 먼저 비교하기](41-admission.md) | [코드·테스트](implementation/41-admission.md) |
| 42 | [모델 옵션의 계층과 단일 재시도 책임](42-options.md) | [코드·테스트](implementation/42-options.md) |
| 43 | [공급자 schema compiler와 가역적 인자 변환](43-provider-contracts.md) | [코드·테스트](implementation/43-provider-contracts.md) |
| 44 | [인자 복원·기본값·제한된 수정 루프](44-argument-repair.md) | [코드·테스트](implementation/44-argument-repair.md) |
| 45 | [문맥 revision과 파생 자료의 권한 철회](45-fragments.md) | [코드·테스트](implementation/45-fragments.md) |
| 46 | [PreparedStep과 ToolSet을 저장하고 재사용하기](46-prepared-step.md) | [코드·테스트](implementation/46-prepared-step.md) |

## 0.2.0: 중단·제어·진단

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 47 | [중단 정책과 업무 app_state](47-interruptions.md) | [코드·테스트](implementation/47-interruptions.md) |
| 48 | [원자적 재개·취소·만료와 불변 handle](48-controls.md) | [코드·테스트](implementation/48-controls.md) |
| 49 | [저장 ID로 조회하는 부작용 없는 진단](49-inspection.md) | [코드·테스트](implementation/49-inspection.md) |

## 0.2.0: 제공자·MCP 연결

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 50 | [OpenAI strict schema와 원본 계약 복원](50-openai-schema.md) | [코드·테스트](implementation/50-openai-schema.md) |
| 51 | [Azure 배포와 strict Tool 계약](51-azure-schema.md) | [코드·테스트](implementation/51-azure-schema.md) |
| 52 | [Anthropic의 잘못된 인자와 서명 replay](52-anthropic-repair.md) | [코드·테스트](implementation/52-anthropic-repair.md) |
| 53 | [Bedrock framing과 재시도 경계](53-bedrock-repair.md) | [코드·테스트](implementation/53-bedrock-repair.md) |
| 54 | [Gemini schema 변환과 JSON bytes 전송](54-gemini-schema.md) | [코드·테스트](implementation/54-gemini-schema.md) |
| 55 | [Vertex 인증과 코어가 소유하는 복구](55-vertex-schema.md) | [코드·테스트](implementation/55-vertex-schema.md) |
| 56 | [xAI 인코딩 설명과 계약 버전 보존](56-xai-contract.md) | [코드·테스트](implementation/56-xai-contract.md) |
| 57 | [MCP 전체 도구 루프의 수정과 Unknown](57-mcp-repair.md) | [코드·테스트](implementation/57-mcp-repair.md) |

## 0.2.0: 통합 회귀·이관·완성

| 장 | 강의 | 전체 구현 |
| --- | --- | --- |
| 58 | [경쟁·프로세스 종료·schema 확장의 통합 회귀](58-recovery-audit.md) | [코드·테스트](implementation/58-recovery-audit.md) |
| 59 | [독립 Host 갱신과 저장 데이터 이관](59-host-migration.md) | [코드·테스트](implementation/59-host-migration.md) |
| 60 | [Wickle 0.2.0 완성과 배포 검증](60-release-v02.md) | [코드·테스트](implementation/60-release-v02.md) |

## 재현 도구

`ko/`에서 `export COURSE="$PWD"`를 설정한다. `python3 "$COURSE/lab.py" snapshot 60 --dest ../wickle-lab-v02`는 존재하지 않는 새 폴더에 처음부터 최종 release까지 복원한다. `check 60 --work 경로`는 최종 workspace 테스트를 실행한다. `compare`는 참조 파일의 byte 비교이며 다른 구현의 의미적 동등성을 대신하지 않는다.

정답 patch에는 manifest·lockfile·문서·binary asset까지 포함돼 Git history 없이 재현할 수 있다. 소스의 라이선스는 [MIT](../reference/LICENSE)다. 패치를 적용하기만 했다는 것은 구현을 이해했다는 뜻이 아니다. 각 장의 예측·결함 실험·해설을 완료한다.
