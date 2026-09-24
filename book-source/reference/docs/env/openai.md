# OpenAI GPT 환경 설정

인증 정보는 한 번 설정하고, 모델별로 정확한 ID와 호출 옵션을 입력합니다. **버전이 포함된 모델 이름을 ID에 넣으면 됩니다. 같은 값을 별도 VERSION에 다시 입력하지 않습니다.**

```dotenv
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1

OPENAI_MODEL_1_ID=
OPENAI_MODEL_1_REASONING_EFFORT=high
OPENAI_MODEL_1_MAX_OUTPUT_TOKENS=4096

# 다른 모델·snapshot 또는 같은 모델의 다른 effort
OPENAI_MODEL_2_ID=
OPENAI_MODEL_2_REASONING_EFFORT=low
OPENAI_MODEL_2_MAX_OUTPUT_TOKENS=4096

# 해당 키에 명시적인 조직·프로젝트 선택이 필요한 경우만 설정
# OPENAI_ORG_ID=
# OPENAI_PROJECT_ID=
```

## 입력할 값

1. [OpenAI Platform](https://platform.openai.com/)의 사용할 프로젝트에서 API key를 만들고 `OPENAI_API_KEY`에 넣습니다. [공식 시작 안내](https://developers.openai.com/api/docs/quickstart).
2. [모델 목록](https://developers.openai.com/api/docs/models)이나 계정의 모델 선택 화면에서 실제 호출할 전체 ID를 `OPENAI_MODEL_1_ID`에 복사합니다. 고정 release를 원하면 해당 모델 페이지의 snapshot ID를 사용합니다. 버전 확인 결과는 테스트가 기록합니다.
3. `REASONING_EFFORT`에 해당 모델이 지원하는 수준을 넣습니다. 지원 값은 모델에 따라 `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max` 중 일부입니다. 모든 모델이 모든 값을 받는 것은 아닙니다. 비워 두면 명시적 effort를 전송하지 않습니다. [Reasoning effort](https://developers.openai.com/api/docs/guides/reasoning#reasoning-effort).
4. `MAX_OUTPUT_TOKENS`는 해당 호출의 출력 예산입니다. 추론 토큰도 사용하는 모델에서는 최종 답변 전에 예산이 소진될 수 있으므로 테스트 목적에 맞게 조정합니다. [추론 모델의 토큰 예산](https://developers.openai.com/api/docs/guides/reasoning).

Responses API의 실제 effort 필드는 `reasoning.effort`입니다. Wickle의 Host 옵션 `reasoning_effort`를 이 구조로 인코딩하는 것은 어댑터의 책임입니다. 모델·API가 지원하지 않는 옵션을 지정하면 오류로 처리하며, 자동으로 무시하지 않습니다. [Responses 요청](https://developers.openai.com/api/reference/resources/responses/methods/create).

`OPENAI_BASE_URL`의 `/v1`은 API 경로이며 모델 버전과 별개입니다. 조직·프로젝트 헤더가 필요한 경우만 추가 값을 설정합니다. [인증과 프로젝트 선택](https://developers.openai.com/api/reference/overview#authentication).

ID가 채워진 슬롯마다 독립 테스트를 실행하도록 준비합니다. 같은 ID와 서로 다른 effort를 넣어 비교할 수도 있습니다. 다른 계정·endpoint는 별도 `.env` 파일을 사용합니다.

.env 설정은 별도의 live 테스트 Host용입니다. 공개 어댑터의 설정·인증 주입 방식은 [OpenAI Responses 사용 안내](../openai.md)를 참고하세요. `.env` 저장만으로 호출이 실행되지 않고 코어 라이브러리는 파일을 읽지 않습니다. [공통 안내](README.md).

## 확인한 현재 모델 계약

`gpt-6-astra`는 Responses의 `reasoning.effort`를 사용하며 `low`, `medium`, `high`, `xhigh`, `max`를 지원합니다. `none`/`minimal`, `temperature`, `top_p`는 이 모델에 사용하지 않습니다. Tool 호출은 Responses를 사용합니다. 이 제한을 다른 모델 전체에 적용하지 않고 모델별 옵션 계약으로 검사합니다. [현재 모델 가이드](https://developers.openai.com/api/docs/guides/latest-model).
