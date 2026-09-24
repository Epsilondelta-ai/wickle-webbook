# AWS Bedrock의 Claude

[환경변수 안내](README.md) · [전체 예제](../../.env.example)

AWS 계정의 인증 정보, 호출을 시작할 리전, 해당 리전에서 사용할 Claude 모델을 준비합니다. Host가 AWS credential provider chain 또는 Bedrock bearer token을 준비하고 어댑터에 명시적으로 전달합니다. 라이브러리가 환경변수나 profile을 암묵적으로 읽는다는 뜻은 아닙니다. Bedrock 어댑터는 명시적 설정으로 사용할 수 있습니다. 아래 환경변수는 테스트 Host의 설정 준비용이며, 실제 연결 검사 runner와 계정별 검증은 별도로 진행합니다.

```dotenv
AWS_REGION=

BEDROCK_MODEL_1_ID=
BEDROCK_MODEL_1_EFFORT=
BEDROCK_MODEL_1_THINKING_MODE=
BEDROCK_MODEL_1_THINKING_BUDGET_TOKENS=
BEDROCK_MODEL_1_MAX_OUTPUT_TOKENS=4096

# 이름 있는 AWS profile을 사용할 때만 설정합니다.
# AWS_PROFILE=
# 임시 access key를 직접 사용할 때만 세 값을 함께 설정합니다.
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
# AWS_SESSION_TOKEN=
# SDK의 기본 리전별 endpoint를 바꿔야 할 때만 설정합니다.
# BEDROCK_ENDPOINT=
```

1. AWS Console에서 계정과 리전을 선택한 뒤 **Amazon Bedrock → Model catalog**에서 Claude 모델의 ID·지원 리전·호출 방식을 확인합니다. 선택한 호출 리전을 `AWS_REGION`에 넣습니다. [지원 모델](https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html).
2. 계정의 모델 접근 조건과 호출 권한을 확인합니다. Bedrock Runtime의 Claude는 최초 사용 정보 제출 등 모델별 접근 준비가 필요할 수 있습니다. 모델 목록 조회 성공만으로 추론 권한까지 확인된 것은 아닙니다. [모델 접근 설정](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html).
3. 인증 방식을 준비합니다. 로컬 IAM Identity Center를 사용한다면 아래처럼 profile을 만들고 로그인합니다. `bedrock-dev`는 직접 정하는 profile 이름이며, 로그인 후 `.env`에 `AWS_PROFILE=bedrock-dev`를 넣습니다. [AWS CLI SSO 설정](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html).

```sh
aws configure sso --profile bedrock-dev
aws sso login --profile bedrock-dev
```

기존 shared profile이나 AWS 실행 환경에 연결된 role도 사용할 수 있습니다. profile·role을 쓸 때는 access key 변수를 주석으로 둡니다. 환경변수의 access key가 profile보다 먼저 선택되므로 셸에 남아 있는 다른 계정의 키도 확인해야 합니다. 임시 키를 직접 사용한다면 access key·secret key·session token 세 값을 함께 준비합니다. [Rust SDK 인증 정보 검색 순서](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html).

모델 목록은 CLI로도 확인할 수 있습니다. `<선택한 리전>`과 profile 이름을 실제 값으로 바꿉니다. role이나 기본 profile을 사용한다면 `--profile` 옵션을 생략합니다.

```sh
aws bedrock list-foundation-models \
  --by-provider Anthropic \
  --region '<선택한 리전>' \
  --profile bedrock-dev \
  --output json

aws bedrock list-inference-profiles \
  --region '<선택한 리전>' \
  --profile bedrock-dev \
  --output json
```

`BEDROCK_MODEL_1_ID`에는 실제 요청의 `modelId`로 사용할 식별자 하나를 입력합니다. Foundation model로 호출하면 그 model ID를, inference profile로 호출하면 해당 profile ID 또는 ARN을 넣습니다. 기반 모델 ID나 release를 별도 입력하지 않으며 임의의 suffix를 만들지 않습니다. [foundation model 목록](https://docs.aws.amazon.com/cli/latest/reference/bedrock/list-foundation-models.html), [profile로 추론하기](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles-use.html).

Inference profile이 가리키는 실제 모델·release·리전은 테스트 검증 단계에서 조회하여 내부 metadata로 기록합니다. 준비 중에는 아래 명령으로도 확인할 수 있습니다. `AWS_REGION`은 요청의 출발 리전입니다. [profile 조회](https://docs.aws.amazon.com/cli/latest/reference/bedrock/get-inference-profile.html).

```sh
aws bedrock get-inference-profile \
  --inference-profile-identifier '<확인한 profile ID 또는 ARN>' \
  --region '<선택한 리전>' \
  --profile bedrock-dev \
  --output json
```

## Effort와 thinking 설정

`BEDROCK_MODEL_1_EFFORT`는 논리 옵션 `effort`입니다. 같은 Claude 계열이라도 모델과 Bedrock API 경로에 따라 지원 여부·허용 수준이 다릅니다. [Claude effort](https://platform.claude.com/docs/en/build-with-claude/effort).

`THINKING_MODE`와 `THINKING_BUDGET_TOKENS`는 각각 `thinking_mode`, `thinking_budget_tokens` 논리 옵션입니다. `adaptive`는 모드이고 effort 값이 아닙니다. 수동 thinking 예산을 지원하는 모델에서만 해당 토큰 설정을 사용합니다. [Adaptive thinking](https://platform.claude.com/docs/en/build-with-claude/adaptive-thinking), [수동 thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking).

빈 옵션은 전송하지 않습니다. 선택한 모델·API의 schema에 없는 값이나 조합은 명시적으로 거부하며, 직접 Claude API 설정을 Bedrock 요청에 그대로 복사하거나 지원하지 않는 옵션을 조용히 무시하지 않습니다. 실제 wire 필드 매핑과 SigV4 서명은 Bedrock 어댑터가 담당합니다. AWS profile·role에서 인증 정보를 얻고 갱신하는 책임은 Host에 있습니다.

`MAX_OUTPUT_TOKENS=4096`은 응답 한 번의 출력 예산 예시입니다. 모델의 요건과 thinking·도구 호출을 포함한 작업량에 맞춰 조정합니다. 긴 tool loop에 충분한 예산을 보장하는 값은 아닙니다.

같은 계정·인증·출발 리전에서 다른 호출 대상을 추가하거나, 같은 `modelId`에 서로 다른 effort를 적용하려면 다음 번호를 사용합니다.

```dotenv
BEDROCK_MODEL_2_ID=
BEDROCK_MODEL_2_EFFORT=
BEDROCK_MODEL_2_THINKING_MODE=
BEDROCK_MODEL_2_THINKING_BUDGET_TOKENS=
BEDROCK_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

번호는 양의 정수이며 ID를 채운 각 번호가 독립된 모델·옵션 검사 대상입니다. 같은 모델의 effort를 비교할 때는 두 슬롯에 같은 실제 호출 ID와 서로 다른 허용 effort 값을 넣습니다. 계정·role/profile·출발 리전·endpoint가 다르면 별도의 `.env` 파일을 사용합니다. 이 설정에는 직접 Anthropic API의 키나 `ANTHROPIC_API_VERSION`을 넣지 않습니다.

## 확인한 현재 모델 계약

Bedrock Claude의 adaptive thinking과 effort는 선택한 model ID와 operation의 계약으로 검사합니다. Messages InvokeModel은 직접 Anthropic 헤더 대신 body의 `anthropic_version: bedrock-2023-05-31`을 사용합니다. Opus 5는 thinking 기본 활성화이며 구형 수동 token-budget 설정을 일괄 적용하지 않습니다. [AWS adaptive thinking](https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-adaptive-thinking.html).

## 최신 Messages 경로와 기존 Runtime 경로

최신 Claude에는 `https://bedrock-mantle.{region}.api.aws/anthropic/v1/messages` 경로도 있습니다. 이 경로는 표준 SSE와 Messages 형식을 사용하며 모델 ID는 `anthropic.claude-opus-5`처럼 provider prefix를 포함합니다. 기존 InvokeModel의 AWS event-stream이나 ARN 버전 문자열을 이 경로의 형식으로 가정하지 않습니다. Native Messages의 기능을 직접 Anthropic API와 동일하게 취급하지 않고 제공 경로별 capability를 별도로 등록해야 합니다. AWS의 세부 문서상 구조화 출력은 일부 모델의 Runtime Converse/InvokeModel 경로에서 지원되지만 Mantle의 `/anthropic/v1/messages`에서는 `output_config.format`을 거부합니다. 같은 Claude라도 모델·operation·endpoint를 함께 확인해야 합니다. [AWS의 구조화 출력 지원 범위](https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-structured-outputs.html).

위의 `anthropic_version: bedrock-2023-05-31` 설명은 **InvokeModel body**에 해당하며 native Messages 요청에 일괄 주입하지 않습니다. 사용하려는 operation과 model/profile을 먼저 선택하고 그 계약에 맞게 Host를 구성합니다. Bedrock bearer token을 쓰는 Host에서만 `AWS_BEARER_TOKEN_BEDROCK`을 선택 입력으로 받을 수 있습니다. 실제 모델 접근 승인이 없으면 연결 검증을 대기 상태로 기록합니다.

[최신 Messages 경로](https://platform.claude.com/docs/en/build-with-claude/claude-in-amazon-bedrock), [기존 Runtime 경로](https://platform.claude.com/docs/en/build-with-claude/claude-on-amazon-bedrock-legacy).
