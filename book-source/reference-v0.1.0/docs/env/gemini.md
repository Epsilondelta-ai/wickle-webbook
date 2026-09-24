# Google AI Studio의 Gemini API

[환경변수 안내](README.md) · [전체 예제](../../.env.example)

Google AI Studio에서 발급한 Gemini API 키와 해당 키로 사용할 모델을 준비합니다. 아래는 모델별 실제 연결 테스트 전용 설정 규약입니다. [Gemini 어댑터](../gemini.md)에 Host가 명시적으로 설정을 전달하며, 실제 계정 연결 검증은 별도로 진행합니다.

```dotenv
GEMINI_API_KEY=
GEMINI_BASE_URL=https://generativelanguage.googleapis.com
GEMINI_API_VERSION=v1

GEMINI_MODEL_1_ID=
GEMINI_MODEL_1_THINKING_LEVEL=
GEMINI_MODEL_1_THINKING_BUDGET_TOKENS=
GEMINI_MODEL_1_MAX_OUTPUT_TOKENS=4096
```

1. [Google AI Studio API Keys](https://aistudio.google.com/apikey)에서 사용할 프로젝트를 선택하고 키를 생성합니다. 기존 프로젝트가 보이지 않으면 **Dashboard → Projects → Import projects**에서 가져옵니다. 키는 해당 Google Cloud 프로젝트의 사용량·결제 설정과 연결됩니다. [프로젝트와 키 준비](https://ai.google.dev/gemini-api/docs/api-key).
2. 새로 생성한 **auth key**를 `GEMINI_API_KEY`에 넣습니다. 공식 문서는 기존 Standard key의 전환을 안내하므로, 오래된 키를 재사용한다면 Key Type과 현재 제한 조건도 확인합니다. Google SDK는 `GOOGLE_API_KEY`가 함께 설정되면 이를 우선할 수 있어, 테스트 실행기는 선택한 `GEMINI_API_KEY`를 SDK 클라이언트에 명시적으로 전달해야 합니다. [키 유형·환경변수 우선순위](https://ai.google.dev/gemini-api/docs/api-key).
3. AI Studio의 모델 선택 화면과 [Models API](https://ai.google.dev/api/models)에서 사용할 모델의 ID·버전·지원 작업을 확인합니다. `name`이 `models/…` 형식이면 `models/` 뒤의 정확한 모델 식별자를 `GEMINI_MODEL_1_ID`에 넣습니다. `supportedGenerationMethods`에 필요한 생성 방식이 있는지도 확인합니다.
4. 모델 식별자는 `GEMINI_MODEL_1_ID` 하나만 입력합니다. 공급자가 공개한 `version`과 ID의 release·alias 의미는 검증 단계에서 확인해 내부 metadata로 기록합니다. 사용자가 버전을 별도로 중복 입력하지 않습니다. [모델 metadata](https://ai.google.dev/api/models#Model).

키를 셸 환경변수에도 준비했다면 공식 모델 목록을 아래처럼 조회할 수 있습니다. 이 명령의 `v1beta`는 목록 조회 경로이며, `.env`의 생성 API 버전을 자동으로 변경하지 않습니다. `.env` 저장만으로 셸 변수가 설정되지는 않습니다.

```sh
curl --fail-with-body --silent --show-error \
  'https://generativelanguage.googleapis.com/v1beta/models' \
  -H "x-goog-api-key: $GEMINI_API_KEY"
```

`nextPageToken`이 있으면 다음 요청에 `pageToken`을 넣어 나머지 모델도 확인합니다. 목록 조회는 Wickle의 실제 생성·Tool 호출 검사를 대신하지 않습니다. [모델 목록 API](https://ai.google.dev/api/models#method:-models.list).

## Thinking 설정

`GEMINI_MODEL_1_THINKING_LEVEL`은 논리 옵션 `thinking_level`이며, generateContent의 `generationConfig.thinkingConfig.thinkingLevel`에 대응합니다. 구형 모델에서 토큰 예산 방식을 사용할 때는 `THINKING_BUDGET_TOKENS`를 `thinking_budget_tokens`로 전달하고 어댑터가 `thinkingBudget`에 매핑합니다. 지원 수준·정수 범위·두 설정의 동시 사용 가능 여부는 선택한 모델·API schema를 확인합니다. [generateContent ThinkingConfig](https://ai.google.dev/api/generate-content#ThinkingConfig).

옵션이 비어 있으면 전송하지 않습니다. 모든 Gemini 모델에 공통된 수준 목록을 가정하지 않으며, 지원되지 않는 설정이나 조합을 명시적으로 거부합니다. 어댑터가 선택한 API의 wire 필드로 변환합니다.

`MAX_OUTPUT_TOKENS=4096`은 한 번의 응답에 대한 출력 예산 예시입니다. 모델의 요건과 thinking·도구 호출에 필요한 토큰을 고려해 조정합니다. 긴 tool loop 전체의 예산을 이 값 하나로 보장하지 않습니다.

같은 프로젝트·키·API 경로에서 두 번째 모델을 추가하거나, 같은 모델 ID에 서로 다른 thinking level을 적용해 비교하려면 다음 슬롯을 사용합니다.

```dotenv
GEMINI_MODEL_2_ID=
GEMINI_MODEL_2_THINKING_LEVEL=
GEMINI_MODEL_2_THINKING_BUDGET_TOKENS=
GEMINI_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

번호는 양의 정수이며 ID를 채운 각 번호가 독립된 모델·옵션 검사 대상입니다. 같은 모델을 비교할 때는 ID를 동일하게 넣고 thinking 설정에 각각 다른 허용값을 입력합니다. 프로젝트·키·endpoint 또는 필요한 API 버전이 다르면 별도의 `.env` 파일을 사용합니다.

`GEMINI_API_VERSION=v1`은 안정 API 경로를 뜻합니다. 선택한 기능이 `v1beta`를 요구한다면 그 기능을 지원하는 어댑터와 함께 설정해야 합니다. 모델 release와 API 버전은 별개입니다. [API 버전](https://ai.google.dev/gemini-api/docs/api-versions). 이 경로의 인증은 AI Studio 키이며, ADC로 준비하는 [Vertex AI 경로](vertex-ai.md)는 별도 설정입니다.

`v1`의 함수 선언은 OpenAPI `parameters`를 사용하며 `additionalProperties: false`를 표현하지 못합니다. Wickle의 일반적인 닫힌 Tool 스키마는 `v1beta`의 `parametersJsonSchema` 경로가 필요합니다. 어댑터는 지원하지 않는 제약을 제거하거나 API 버전을 자동으로 바꾸지 않고 호출 전에 오류를 반환합니다. 텍스트·thinking 지원과 Tool 스키마 지원은 별도로 확인합니다. [버전별 지원 범위](../gemini.md#api-versions-and-schemas).

## 확인한 현재 모델 계약

`gemini-3.8-flash`의 thinking level은 `low`, `medium`, `high`이고 기본값은 `medium`입니다. `minimal`은 지원하지 않습니다. 모델 리소스 이름 `models/gemini-3.8-flash`도 입력할 수 있으며 어댑터가 `models/`를 중복해 붙이지 않아야 합니다. `v1`의 공식 discovery schema에도 `ThinkingConfig.thinkingLevel`이 있으므로 beta 예제가 있다는 이유만으로 설정을 `v1beta`로 바꾸지 않습니다. generateContent와 Interactions는 서로 다른 wire 계약입니다. [모델](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash), [현재 모델 사용법](https://ai.google.dev/gemini-api/docs/latest-model).
