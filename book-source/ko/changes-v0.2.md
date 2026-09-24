# 0.1.0 → 0.2.0 학습 변경 지도

[목차](README.md)

최신 교재는 기존 34개 기초 checkpoint에 24개 개선 checkpoint를 이어 붙인다. 03–36장은 0.1.0 의미를 설명하고 37–60장은 실제 변경 순서로 그 의미를 바꾼다. 기존 장의 설명을 최종 계약으로 오해하지 않도록 각 장 맨 앞에 변경 안내를 넣었다. 이 방식은 중간 빌드가 가능한 구현 순서를 보존한다.

| 기존 강의 | 0.2.0 의미 변경 | 새 강의 |
| --- | --- | --- |
| [04장](04-contracts.md) | 새 JSON text encoder는 숫자 lexeme를 보존하며 과거 sorted-json-v1 digest와 구분한다. 새 저장 기록을 추가해도 Profile의 v1 schema ID를 일괄 변경하지 않는다. | [38](38-canonical-json.md), [39](39-execution-contracts.md) |
| [05장](05-policy.md) | 재개 요청자와 원래 execution principal을 분리한다. 진단도 현재 권한을 검사하며 read-only view가 만료 처리를 대신하지 않는다. | [48](48-controls.md), [49](49-inspection.md) |
| [06장](06-state.md) | StateStore는 필수 ExecutionTransactions를 같은 원자 저장 경계에서 구현한다. command 소비·새 segment·lease를 서로 다른 write로 나누면 안 된다. | [39](39-execution-contracts.md), [40](40-atomic-store.md), [48](48-controls.md) |
| [07장](07-budget.md) | transport retry/fallback, ToolRepair, 품질 보완을 구분한다. ToolRepair는 잘못된 response round당 한 번 예약하고 다음 모델 호출은 model budget을 따로 쓴다. | [42](42-options.md), [44](44-argument-repair.md), [46](46-prepared-step.md) |
| [08장](08-model.md) | 완전한 응답 안의 잘못된 Tool 인자는 raw 원문과 repairable proposal로 보존한다. stream envelope 손상·미완료와 혼동하지 않는다. 준비 기록과 actual attempt도 구분한다. | [43](43-provider-contracts.md), [44](44-argument-repair.md), [46](46-prepared-step.md), [50](50-openai-schema.md), [52](52-anthropic-repair.md) |
| [09장](09-schema.md) | canonical schema와 provider wire schema를 분리한다. 숨은 system 의존 조건은 전체 실행 validator에 남기고 model 조건만 투영한다. provider 미지원만으로 Tool을 삭제하지 않는다. | [43](43-provider-contracts.md), [44](44-argument-repair.md) |
| [10장](10-context.md) | 원본 transcript boundary, fragment lineage, ToolSet, compiled contract를 PreparedStep에 고정한다. 새 compiler로 과거 준비를 재생성하지 않는다. | [45](45-fragments.md), [46](46-prepared-step.md) |
| [11장](11-binding.md) | 0.2.0에서는 누락된 최상위 모델 필드의 direct/local-ref default를 required 여부와 무관하게 먼저 적용한다. 명시 null·중첩·system default는 추정하지 않는다. 재개 reviewer가 원 execution actor를 대체하지 않는다. | [44](44-argument-repair.md), [48](48-controls.md) |
| [12장](12-catalog.md) | ModelBinding.default_options → AgentProfile.model_options → RunRequest.model_options의 최상위 교체와 출처·schema revision을 저장한다. | [42](42-options.md) |
| [13장](13-sqlite.md) | 읽기는 구 checkpoint를 바꾸지 않지만 지원되는 새 admission은 v2 checkpoint로 원자 upgrade한다. 따라서 과거의 “migration 없음” 설명을 최종 0.2.0에 적용하지 않는다. legacy active는 drain한다. | [40](40-atomic-store.md), [59](59-host-migration.md) |
| [14장](14-routing.md) | 같은 입력 retry는 저장 preparation을 재사용한다. fallback은 새로운 projection revision으로 남기고 대상 schema를 다시 검증한다. 반복 retry를 SDK에 숨기지 않는다. | [42](42-options.md), [46](46-prepared-step.md) |
| [15장](15-agent.md) | AgentBindings에 interruption_policy가 추가되고 생성 시 등록 해석 일부가 신규 start로 지연된다. handle은 영속 segment identity를 갖고 inspect_step/control API가 추가된다. | [41](41-admission.md), [47](47-interruptions.md), [48](48-controls.md), [49](49-inspection.md), [59](59-host-migration.md) |
| [16장](16-tools.md) | Tool 실행은 원래 광고한 ToolSet·compiled contract에 결합한다. raw→decode→default/validation→Hook→재검증→system bind 순서로 바뀐다. | [44](44-argument-repair.md), [46](46-prepared-step.md) |
| [17장](17-resume.md) | 과거 “다른 검토자가 current principal을 갱신한다”는 설명은 원래 execution actor 보존 계약으로 바뀐다. idle cancel도 새 control-only segment를 만들므로 옛 Waiting handle outcome은 그대로다. | [48](48-controls.md) |
| [18장](18-hooks.md) | 모델 기본값은 Hook 이전과 이후에 정규화하며 새 Hook output은 정규화 후 저장한다. 과거 Hook 기록은 당시 입력 그대로 재생한다. 파생 자료의 권한을 재검사한다. | [44](44-argument-repair.md), [45](45-fragments.md) |
| [19장](19-adapters.md) | 중단 settlement·lease 해제·adapter close의 순서와 cleanup window를 구분한다. 원 실행자와 현재 명령 제출자·정리 관찰자를 혼합하지 않는다. | [47](47-interruptions.md), [48](48-controls.md) |
| [20장](20-sources.md) | core_revision/content_digest/source_revision을 분리하고 Empty·Unavailable·Deleted와 lineage의 의미를 구분한다. 삭제된 원천을 요약으로 우회할 수 없다. | [45](45-fragments.md) |
| [21장](21-skills.md) | Skills·Artifact의 권한과 원천 의존성을 saved preparation에서도 확인한다. 과거 transcript boundary를 이후 메시지로 바꾸지 않는다. | [45](45-fragments.md), [46](46-prepared-step.md) |
| [22장](22-compaction.md) | ModelCompactor options:None을 agent Run 옵션 자동 상속으로 설명한 옛 규칙은 바뀐다. auxiliary purpose 설정을 독립적으로 고정하며 요약의 원천 권한도 다시 검사한다. | [42](42-options.md), [45](45-fragments.md), [46](46-prepared-step.md) |
| [23장](23-verification.md) | 검증 목적 옵션과 agent 목적 옵션을 분리한다. candidate 승인·Input 명령·새 segment 수락의 원자성을 확인하고 이전 결과를 다시 쓰지 않는다. | [42](42-options.md), [45](45-fragments.md), [48](48-controls.md) |
| [24장](24-recovery.md) | Interrupted와 legacy active drain, 저장 preparation 복구, command acceptance·새 segment·lease 원자 전이를 함께 다룬다. 구 기록에 없던 actor·boundary를 만들어 재개하지 않는다. | [40](40-atomic-store.md), [46](46-prepared-step.md), [47](47-interruptions.md), [48](48-controls.md), [58](58-recovery-audit.md) |
| [25장](25-openai.md) | strict provider schema를 가역 compiler로 생성하고 추가 제약을 설명한다. 완전 invalid args는 repair 대상으로 보존하며 최종 Responses compiler revision 2는 expansion work를 제한한다. | [50](50-openai-schema.md), [58](58-recovery-audit.md) |
| [26장](26-azure.md) | Azure 정책 한도와 underlying model identity를 분리한다. root field 수 초과는 JsonObjectText, compiler revision 2는 공유 확장 budget을 사용한다. | [51](51-azure-schema.md), [58](58-recovery-audit.md) |
| [27장](27-anthropic.md) | Native schema는 유지하고 invalid raw Tool inputs만 새 replay envelope로 보존한다. 정상 v1 continuation·서명은 그대로 유지한다. | [52](52-anthropic-repair.md) |
| [28장](28-bedrock.md) | HTTP 424 전체가 아니라 명시 ModelStreamErrorException만 Transport로 분류한다. binary frame과 HTTP 경로의 core-owned retry를 함께 검사한다. | [53](53-bedrock-repair.md) |
| [29장](29-gemini.md) | provider native schema로 표현 못하는 제약은 core 검증과 문맥으로 보존한다. codec 반환형 Vec<u8>는 HTTP body로 직접 전송한다. | [54](54-gemini-schema.md) |
| [30장](30-vertex.md) | 공유 Gemini compiler·raw bytes 경로를 연결하되 OAuth/project/location의 독립 경계는 유지한다. 릴리스 후 gcloud OAuth live 기록은 별도 자료다. | [55](55-vertex-schema.md) |
| [31장](31-xai.md) | xAI native/JsonText 의미와 불필요한 strict flag를 정리한다. 계약 v2의 인코딩별 설명과 저장 v1 정확 복원을 구분한다. | [56](56-xai-contract.md) |
| [32장](32-mcp.md) | 새 MCP 병렬 schema 엔진을 만들지 않고 실제 Agent→binder→stdio 전체 경로를 검증한다. invalid proposals에서 remote 실행 0회, 불확실한 쓰기 재실행 금지를 유지한다. | [57](57-mcp-repair.md) |
| [33장](33-events.md) | 외부 delivery와 Run control command는 다른 원장이다. durable command 저장만으로 remote worker에 알림이 전달되는 것은 아니다. | [48](48-controls.md) |
| [34장](34-evidence.md) | 릴리스별 contract·live 증거를 분리한다. v0.1 통과를 바뀐 schema/compiler/옵션 경로의 검증으로 승계하지 않는다. | [50](50-openai-schema.md), [51](51-azure-schema.md), [52](52-anthropic-repair.md), [53](53-bedrock-repair.md), [54](54-gemini-schema.md), [55](55-vertex-schema.md), [56](56-xai-contract.md), [60](60-release-v02.md) |
| [35장](35-integration.md) | 기본 debug에서의 nested Future·공정성 회귀와 source archive 전체 소비자를 검사한다. 선택 consumer만으로 다른 include 의존자의 빌드를 보장하지 않는다. | [58](58-recovery-audit.md), [59](59-host-migration.md) |
| [36장](36-release.md) | 이 장은 기초 0.1.0 checkpoint 완성이다. 전체 학습 목표인 0.2.0은 37–60장을 완료한 뒤 최종 평가한다. | [37](37-compatibility.md), [59](59-host-migration.md), [60](60-release-v02.md) |

## 세 버전 축을 혼동하지 않기

라이브러리 0.2.0, `wickle.state-store.v2`, 여전히 사용되는 `wickle.agent-profile.v1`, compiler revision 2, provider model release는 서로 다른 축이다. 전체 파일에서 0.1/v1을 0.2/v2로 일괄 치환하지 않는다. 저장된 record는 당시 encoder·설명·원문·codec으로 복원한다.

## 구현 순서가 작업 번호와 다른 이유

fragment 구현은 PreparedStep 통합보다 먼저 머지됐다. 이 교재도 실제 dependency와 태그 history에 맞춰 45장 fragment → 46장 PreparedStep으로 배치했다. 계획서의 번호만 따라 patch를 적용하면 중간 타입·의존성이 맞지 않을 수 있다.

## 최종 범위

13개 crate는 그대로지만 실행/저장/진단·provider codec과 Host 구성이 확장됐다. 별도 SaaS UI·원격 worker 전달 서비스·범용 보상 거래 엔진을 새로 구현한 것은 아니다. 최종 전체 367개 파일은 `../reference/`, 기초 0.1.0 참조는 `../reference-v0.1.0/`에 분리했다.
