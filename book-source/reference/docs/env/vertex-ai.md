# Google Cloud Vertex AI의 Gemini

[환경변수 안내](README.md) · [전체 예제](../../.env.example)

Google Cloud 프로젝트와 Application Default Credentials(ADC)를 준비합니다. 모델 테스트 Host는 기본적으로 `global` endpoint를 사용하므로 위치 환경변수는 필요하지 않습니다. 여기서는 ADC 경로를 설정합니다. [Vertex 어댑터](../vertex.md)에 Host가 인증·프로젝트 설정을 명시적으로 전달합니다. 실제 연결 검사 runner와 계정 검증은 별도로 진행하며, 코어는 아래 `.env`를 읽지 않습니다.

```dotenv
GOOGLE_CLOUD_PROJECT=
VERTEX_API_VERSION=v1

VERTEX_MODEL_1_ID=
VERTEX_MODEL_1_THINKING_LEVEL=
VERTEX_MODEL_1_THINKING_BUDGET_TOKENS=
VERTEX_MODEL_1_MAX_OUTPUT_TOKENS=4096

# 특정 credential/federation 설정 파일을 선택할 때만 설정합니다.
# GOOGLE_APPLICATION_CREDENTIALS=
# Host에서 별도 quota project를 지정해야 할 때만 설정합니다.
# GOOGLE_CLOUD_QUOTA_PROJECT=
# Host에서 기본 global endpoint 대신 사용할 origin이 있을 때만 설정합니다.
# VERTEX_ENDPOINT=
```

1. Google Cloud Console에서 사용할 프로젝트를 선택합니다. **프로젝트 ID**를 `GOOGLE_CLOUD_PROJECT`에 복사하고, 결제 연결과 `aiplatform.googleapis.com` 활성화를 확인합니다. 호출 주체에는 `roles/aiplatform.user` 또는 필요한 추론 권한을 담은 별도 역할이 필요합니다. [프로젝트·권한 준비](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/start/quickstart).
2. Console의 모델 카탈로그에서 Gemini 모델을 선택한 뒤 [모델 ID·release 목록](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/learn/model-versions)과 [지원 위치](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/learn/locations)를 확인합니다. 현재 `gemini-3.8-flash` 테스트는 `global`을 선택합니다. 별도 환경변수 입력을 요구하지 않고 Host가 요청 경로의 `locations/global`을 구성합니다. 해당 모델의 공식 지원 위치는 `global`, `us`, `eu`입니다. 다른 위치가 필요한 애플리케이션은 Host 연결 옵션으로 명시하고 모델별 지원 위치를 검사합니다. [현재 모델의 지원 위치](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-8-flash).
3. 모델 요청에 사용할 정확한 Google 모델 ID를 `VERTEX_MODEL_1_ID` 하나에 복사합니다. 표시 이름·프로젝트 ID·endpoint ID를 모델 ID 대신 넣지 않습니다. 실제 release와 고정 여부는 테스트 검증 단계에서 조회하여 내부 metadata로 기록하므로 별도 버전을 중복 입력하지 않습니다. [모델 버전과 수명주기](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/learn/model-versions).

CLI에서 접근 가능한 프로젝트를 조회하고 로컬 ADC를 준비할 수도 있습니다. API 활성화 명령의 프로젝트 ID는 실제 값으로 바꿉니다.

```sh
gcloud projects list --format='table(projectId,name)'
gcloud services enable aiplatform.googleapis.com --project '<프로젝트 ID>'
gcloud auth application-default login
```

`gcloud projects list`의 프로젝트 ID와 Console의 선택값이 같은지 확인합니다. `gcloud auth login`의 CLI 로그인과 애플리케이션이 사용하는 ADC 설정은 구분되므로, 로컬 라이브러리용으로는 `application-default login`을 사용합니다. [프로젝트 조회](https://docs.cloud.google.com/sdk/gcloud/reference/projects/list), [ADC 준비](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/start/gcp-auth).

로컬 ADC 또는 실행 환경에 연결된 service account를 사용할 때는 `GOOGLE_APPLICATION_CREDENTIALS`를 설정하지 않습니다. 특정 credential/federation 파일을 의도적으로 사용할 경우에만 그 파일 경로를 넣고 파일은 저장소 밖에 보관합니다. ADC는 지정 파일, 로컬 ADC, 연결된 실행 환경의 identity 순으로 인증 정보를 찾습니다. [ADC 검색 순서](https://docs.cloud.google.com/docs/authentication/application-default-credentials).

별도 quota project가 필요하면 ADC의 quota project를 설정합니다. `GOOGLE_CLOUD_QUOTA_PROJECT`를 읽어 적용하는 부분은 Host 인증 라이브러리의 연결 책임이며, 환경변수 작성만으로 모든 SDK 설정이 바뀌는 것은 아닙니다. [quota project 설정](https://docs.cloud.google.com/docs/quotas/set-quota-project).

```sh
gcloud auth application-default set-quota-project '<quota 프로젝트 ID>'
```

## Thinking 설정

`VERTEX_MODEL_1_THINKING_LEVEL`은 논리 옵션 `thinking_level`입니다. 구형 모델의 토큰 예산 방식에는 `THINKING_BUDGET_TOKENS`를 사용해 `thinking_budget_tokens`를 전달합니다. 어댑터가 선택된 Vertex API의 `thinkingConfig`에 매핑하며, 지원 level·예산 범위·동시 설정 제약은 모델과 API별로 검사합니다. [Vertex Gemini thinking](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/thinking).

빈 옵션은 전송하지 않습니다. 모든 모델에 공통 enum을 가정하거나 지원하지 않는 설정을 조용히 버리지 않고 명시적으로 거부합니다. Vertex 어댑터가 선택한 v1 wire 계약으로 변환합니다. ADC에서 토큰을 얻고 갱신하는 책임은 Host에 있습니다.

`MAX_OUTPUT_TOKENS=4096`은 응답 한 번의 출력 예산 예시입니다. 모델별 호출 요건과 thinking·도구 호출에 필요한 공간에 맞춰 조정합니다. 긴 tool loop에 충분한 전체 예산을 뜻하지 않습니다.

같은 프로젝트·ADC·위치에서 다른 모델을 추가하거나, 동일한 모델 ID에 서로 다른 thinking level을 적용해 비교하려면 다음 번호를 사용합니다.

```dotenv
VERTEX_MODEL_2_ID=
VERTEX_MODEL_2_THINKING_LEVEL=
VERTEX_MODEL_2_THINKING_BUDGET_TOKENS=
VERTEX_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

번호는 양의 정수이며 ID를 채운 각 번호가 독립된 모델·옵션 검사 대상입니다. 같은 모델을 비교할 때는 ID를 동일하게 넣고 thinking 설정에 각각 다른 허용값을 입력합니다. 프로젝트·인증 계정·리전·endpoint가 다르면 별도의 `.env` 파일을 사용합니다.

`VERTEX_API_VERSION=v1`은 API 계약 버전입니다. 모델 버전과는 별개이며, 기본 테스트 endpoint는 `https://aiplatform.googleapis.com`이며 resource path에 `locations/global`을 사용합니다. [공식 호출 예제](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/start/quickstart). AI Studio 키를 준비한 경우에는 [Gemini API 가이드](gemini.md)의 경로를 사용합니다.
