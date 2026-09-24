# 모델 테스트용 `.env` 설정

접근 가능한 프로바이더의 인증 정보와 **실제 호출할 모델 ID 또는 배포명**을 준비합니다. 버전이 포함된 모델 ID는 그대로 한 번만 입력합니다. 별도의 모델 `VERSION` 입력은 없습니다. 모델별 effort/thinking 수준과 출력 토큰 한도도 설정할 수 있습니다.

**이 파일과 환경변수는 실제 연결 테스트 전용입니다. Wickle 라이브러리는 `.env`를 읽지 않으며, 실제 서비스에서는 Host가 구성한 어댑터와 호출 옵션을 전달합니다.** 제공 경로 어댑터와 live test runner는 구현 중입니다.

## 프로바이더별 가이드

| 경로 | 호출 대상 | 추론 조절 옵션 |
| --- | --- | --- |
| [OpenAI GPT](openai.md) | `OPENAI_MODEL_<N>_ID` | `REASONING_EFFORT` |
| [Azure OpenAI GPT](azure-openai.md) | `AZURE_OPENAI_MODEL_<N>_DEPLOYMENT` | `REASONING_EFFORT` |
| [Anthropic Claude](anthropic.md) | `ANTHROPIC_MODEL_<N>_ID` | `EFFORT`, 필요한 모델의 thinking 설정 |
| [AWS Bedrock Claude](bedrock.md) | `BEDROCK_MODEL_<N>_ID` | `EFFORT`, 필요한 모델의 thinking 설정 |
| [Gemini API](gemini.md) | `GEMINI_MODEL_<N>_ID` | `THINKING_LEVEL` 또는 지원되는 thinking token budget |
| [Vertex AI Gemini](vertex-ai.md) | `VERTEX_MODEL_<N>_ID` | `THINKING_LEVEL` 또는 지원되는 thinking token budget |
| [xAI Grok](xai.md) | `XAI_MODEL_<N>_ID` | `REASONING_EFFORT` |

추론 옵션 이름은 각 `MODEL_<N>_` 뒤에 붙입니다. 모든 경로에서 같은 번호의 `MAX_OUTPUT_TOKENS`로 출력 토큰 한도를 정합니다. 지원 옵션과 값은 모델·API별로 다릅니다. 빈 옵션은 전송하지 않고, 지정한 미지원 옵션은 오류로 처리합니다.

## 파일 만들기

Wickle 저장소의 [`.env.example`](../../.env.example)을 참고해 `.env`를 만듭니다. 기존 파일이 있으면 필요한 항목만 편집합니다.

```sh
if [ ! -e .env ]; then
  (umask 077; cp .env.example .env)
fi
```

`.env`와 `.env.*`는 Git에서 제외됩니다. 실제 키와 credential JSON은 커밋하지 않습니다. `.env`와 `.env.example`은 배포용 `.crate`에도 포함하지 않습니다.

## 모델과 effort별로 설정하기

```dotenv
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1

OPENAI_MODEL_1_ID=
OPENAI_MODEL_1_REASONING_EFFORT=high
OPENAI_MODEL_1_MAX_OUTPUT_TOKENS=4096

OPENAI_MODEL_2_ID=
OPENAI_MODEL_2_REASONING_EFFORT=low
OPENAI_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

각 ID 칸에 접근 가능한 정확한 모델 이름을 넣습니다. 두 칸에 같은 모델 ID를 넣으면 effort별 비교가 되고, 서로 다른 모델 또는 snapshot ID를 넣으면 모델·버전별 비교가 됩니다. 위 `high`와 `low`도 선택한 모델이 지원할 때만 사용합니다.

- 모델 ID가 입력된 각 번호가 독립 테스트 대상입니다. Azure는 배포명이 기준입니다. 공급자를 켜는 별도 변수는 없습니다.
- 번호는 프로바이더별로 `1`, `2`, `3`처럼 늘립니다. 인증 정보는 한 번만 작성합니다.
- `MAX_OUTPUT_TOKENS=4096`은 예시 출력 예산입니다. 큰 effort에서 출력이 한도에 걸리면 테스트 목적과 모델 한도에 맞춰 조정합니다.
- API 버전, region, endpoint, 인증은 실제 연결에 필요한 값이므로 해당 가이드를 따릅니다. 계정·리소스·region이 다르면 별도 `.env` 파일로 나눕니다.
- 모델 버전과 고정 여부는 모델 ID·배포 metadata를 확인해 결과에 기록합니다. 이름에서 날짜를 만들거나 요청값을 공급자가 보고한 버전으로 복사하지 않습니다.

이전 설정에서 `*_MODEL_<N>_VERSION`은 삭제합니다. Azure는 실제 배포명만 유지하고, Bedrock은 profile로 호출한다면 그 profile ID/ARN을 `BEDROCK_MODEL_<N>_ID`에 넣습니다. 별도의 기반 모델 ID를 중복 입력하지 않습니다.

## 호출 옵션 전달

테스트 실행기는 effort/thinking 값을 Host 모델 옵션으로 구성하고, 출력 한도는 모델 요청의 별도 token budget으로 전달합니다. `REASONING_EFFORT`는 `reasoning_effort`, `EFFORT`는 `effort`, thinking 설정은 `thinking_mode`·`thinking_level`·`thinking_budget_tokens`라는 옵션 키로 다룹니다. 해당 키는 선택한 모델 binding의 schema가 허용해야 합니다.

이 옵션 map을 HTTP body에 임의로 합치지 않습니다. 어댑터가 해당 모델·API의 필드로 변환합니다. 모델별 지원 검사는 옵션을 조용히 제거하거나 다른 수준으로 바꾸지 않고 실제 전송과 응답까지 확인해야 합니다.

파일 준비 후 파일 경로와 작성한 모델 항목을 알려주면 됩니다. 비밀 키 자체를 메시지로 보낼 필요는 없습니다. 설정 누락·미지원 옵션·호출 실패·미확인 버전을 성공으로 기록하지 않습니다.
