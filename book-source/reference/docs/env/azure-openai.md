# Azure Foundry OpenAI GPT 환경 설정

Azure에서는 **실제 호출할 deployment 이름**과 그 호출의 effort·출력 예산을 입력합니다. 기반 모델 ID와 버전을 별도 환경변수로 중복 입력하지 않습니다.

```dotenv
# 리소스 origin. /openai/v1/ 또는 deployment 경로는 붙이지 않습니다.
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_AUTH_MODE=api_key
AZURE_OPENAI_API_KEY=

AZURE_OPENAI_API_MODE=v1
# dated API 경로에서만 해당 프로토콜 버전을 입력
AZURE_OPENAI_API_VERSION=

AZURE_OPENAI_MODEL_1_DEPLOYMENT=
AZURE_OPENAI_MODEL_1_REASONING_EFFORT=high
AZURE_OPENAI_MODEL_1_MAX_OUTPUT_TOKENS=4096

AZURE_OPENAI_MODEL_2_DEPLOYMENT=
AZURE_OPENAI_MODEL_2_REASONING_EFFORT=low
AZURE_OPENAI_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

## 입력할 값

1. [Azure Portal](https://portal.azure.com/)의 OpenAI 리소스에서 **Keys and endpoint**를 확인합니다. endpoint origin과 key를 위 공통 항목에 넣습니다. [리소스 인증 정보](https://learn.microsoft.com/en-us/connectors/azureopenai/#get-your-credentials).
2. [Foundry](https://ai.azure.com/)에서 사용할 배포의 이름을 `MODEL_1_DEPLOYMENT`에 복사합니다. 추론 요청의 `model`에는 이 배포 이름이 들어갑니다. 기반 모델과 현재 버전·업그레이드 정책은 배포 metadata 검증 결과로 기록합니다. [모델과 배포](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/working-with-models), [모델 버전](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/concepts/model-versions).
3. `REASONING_EFFORT`는 해당 배포 모델·API가 지원하는 수준을 입력합니다. 허용 수준은 모델별로 다르며 빈 값은 미전송입니다. `MAX_OUTPUT_TOKENS`는 모델의 한도 안에서 정합니다. [Azure reasoning 모델](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/reasoning?view=foundry-classic).

같은 배포와 서로 다른 effort를 두 슬롯에 넣어 비교할 수 있습니다. 모델 버전 두 개를 비교할 때는 각 버전을 배포한 서로 다른 deployment를 선택합니다. 배포 이름이 고정돼 있어도 기반 모델은 업그레이드될 수 있으므로, 확인할 수 없는 버전을 pinned 또는 live 검증 완료로 기록하지 않습니다.

`v1`에서는 리소스 주소에 `/openai/v1/`을 붙여 호출하고 날짜 형식의 `api-version`은 비워둡니다. dated API를 지원하는 어댑터 경로를 선택할 때만 `AZURE_OPENAI_API_MODE=dated`와 정확한 `AZURE_OPENAI_API_VERSION`을 설정합니다. 이 값은 모델 버전이 아닙니다. [API 버전](https://learn.microsoft.com/en-us/azure/foundry/openai/api-version-lifecycle?view=foundry-classic).

## Microsoft Entra ID

Entra 인증을 사용할 때는 `AZURE_OPENAI_AUTH_MODE=entra`로 바꾸고 API key를 비웁니다. 로컬 Azure CLI 로그인이나 관리 ID를 사용할 수 있습니다. 서비스 주체를 직접 구성하는 경우에만 tenant·client·secret을 설정합니다. 토큰 발급·갱신은 Host의 인증 제공자가 처리합니다. [Entra 인증](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/how-to/configure-entra-id).

```dotenv
AZURE_OPENAI_AUTH_MODE=entra
AZURE_OPENAI_API_KEY=
# AZURE_TENANT_ID=
# AZURE_CLIENT_ID=
# AZURE_CLIENT_SECRET=
# AZURE_OPENAI_ENTRA_SCOPE=https://ai.azure.com/.default
```

배포명이 채워진 번호가 독립 테스트 대상입니다. 다른 리소스·계정·API 설정은 별도 `.env` 파일로 나눕니다. 기반 모델 metadata 조회에 추가 권한이 필요한 경우에는 그 검증 상태를 별도로 기록하며, 확인하지 못한 정보를 사용자가 추측해서 채우도록 요구하지 않습니다.

이 설정은 live 테스트 Host 전용입니다. 라이브러리는 `.env`를 읽지 않으며 [Azure OpenAI 어댑터](../azure-openai.md)에 명시적인 연결과 credential provider를 전달합니다. 현재 구현은 resource-level Responses v1만 지원하므로 `API_MODE=v1`을 사용합니다. dated API 값은 지원되지 않습니다. [공통 안내](README.md).

ARM 배포 metadata를 실제로 검증하려면 다음 account resource ID와 해당 리소스의 deployment 읽기 권한을 가진 management Entra 인증이 추가로 필요합니다. inference API key를 management API에 보내지 않습니다.

```dotenv
AZURE_OPENAI_RESOURCE_ID=/subscriptions/<subscription>/resourceGroups/<group>/providers/Microsoft.CognitiveServices/accounts/<account>
```

관리 API 인증은 Host의 별도 credential provider가 `https://management.azure.com/.default` 대상 토큰을 준비합니다. 모델 버전을 사용자가 추측해서 환경변수에 중복 입력하지 않습니다. 실제 인증·배포가 없는 경우 로컬 fixture만 통과한 것으로 기록하며 live 검증 통과로 표시하지 않습니다.

## 확인한 현재 모델 계약

Azure의 `gpt-6-astra` 배포도 `none`, `temperature`, `top_p`를 지원하지 않습니다. Azure는 현재 mid-conversation `configuration_update` 및 `response.steer`를 지원하지 않으므로 OpenAI 직접 API와 기능을 동일하게 취급하지 않습니다. [Azure reasoning 모델 문서](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/reasoning).
