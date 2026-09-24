# Wickle

[English](README.md) | **한국어** | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

**Rust 애플리케이션에 내장하는 확장형 에이전트 엔진.**

프로필, 모델, 도구를 조합해 애플리케이션 안에서 에이전트를 실행합니다. Wickle은 같은 프로세스에서 모델 판단과 도구 호출을 반복하며, 데이터 접근·인증 정보·권한 정책은 애플리케이션이 제공합니다.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## 설치

Rust 1.85 이상과 Tokio 런타임이 필요합니다. 코어와 선택한 어댑터는 같은 Git 태그로 설치합니다. v0.1.0은 GitHub Release로 배포합니다.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
```

## 에이전트 실행

`AgentProfile`에 지시문, 사용할 도구, 모델 바인딩, 실행 한도를 지정합니다. 모델·상태 저장소·권한 정책 등 애플리케이션 구성요소로 `AgentBindings`를 만든 뒤, 인증된 `ExecutionContext`와 함께 `RunRequest`를 전달합니다.

```rust
use wickle::*;

pub async fn run_once(
    profile: AgentProfile,
    bindings: AgentBindings,
    request: RunRequest,
    context: ExecutionContext,
) -> Result<Guarded<RunOutcome>, ContractError> {
    let agent = create_agent(profile, bindings)?;
    match agent.start(request, context.clone()).await? {
        Guarded::Completed(handle) => handle.outcome(&context).await,
        Guarded::ApprovalRequired(challenge) => {
            Ok(Guarded::ApprovalRequired(challenge))
        }
    }
}
```

`start`는 실행 핸들을 반환하고, `outcome`은 저장된 결과를 반환합니다. `Guarded::ApprovalRequired`이면 애플리케이션에서 승인을 받아야 합니다. 이 예제의 구성요소는 호출하는 애플리케이션에서 준비하며, 바인딩 설정은 에이전트 실행 가이드를 참고하세요.

## 주요 기능

- **도구 입력 분리:** 모델이 작성할 인자만 노출하고, workspace ID·user ID 등 신뢰할 값은 등록된 시스템 입력으로 주입합니다.
- **실행 상태 저장:** 결과와 이벤트를 저장하고 승인·입력 대기, 명시적 재개, 중단된 실행 복구를 지원합니다.
- **확장 기능:** 도구, 읽기 전용 ContextSource, Skills, lifecycle hook, MCP stdio 도구를 연결합니다.
- **실행 한도:** 모델 호출·도구 시도·보완·경과 시간을 제한하며 압축과 검증에도 같은 실행 예산을 적용합니다.
- **범위별 접근 제어:** Host 정책과 바인딩으로 조직·워크스페이스의 접근 범위를 구분합니다.
- **문맥과 결과 처리:** 원본 artifact와 근거를 유지하고 문맥 압축, 구조화 출력 검사, verifier 기반 검증을 수행합니다.

## 모델 제공자와 어댑터

다음 서비스를 선택형 crate로 연결할 수 있습니다. 모델 버전, 배포 이름, API 계약, 제공자별 옵션을 명시적으로 설정합니다. 지원 작업과 모델별 제약은 제공자 가이드를 확인하세요.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

로컬 영속 저장에는 `wickle-state-sqlite`, 범위별 확장 구성에는 `wickle-adapter-runtime`을 사용합니다. 메모리·그래프 서비스는 ContextSource나 도구로 연결하며, 실행 후 기록 갱신은 외부 이벤트 소비자가 담당합니다.

## 사용 문서

- [설치와 의존성 구성](docs/installation.md)
- [에이전트 바인딩·요청·결과](docs/agents.md)
- [도구와 시스템 입력](docs/tool-inputs.md)
- [문맥 조회와 메모리](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [모델 라우팅](docs/model-routing.md)
- [모델 제공자 설정과 지원 범위](docs/model-providers.md)
- [SQLite 저장과 복구](docs/sqlite-state-store.md)
- [MCP 도구](docs/mcp.md)
- [Artifacts](docs/artifacts.md)
- [문맥 압축](docs/context-compaction.md)
- [출력 검증](docs/verification.md)

## 라이선스

[MIT](LICENSE) · MIT © EpsilonDelta. 0.1 계열은 초기 API로, 이후 버전에서 변경될 수 있습니다.
