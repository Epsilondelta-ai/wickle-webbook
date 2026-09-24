# Anthropic Claude 직접 API

[환경변수 안내](README.md) · [전체 예제](../../.env.example)

Claude Console에서 발급한 API 키와 직접 API의 모델 ID를 준비합니다. 아래 설정은 모델별 실제 연결 테스트 전용 규약입니다. Wickle 코어가 `.env`를 자동으로 읽지는 않으며, [Messages 어댑터](../anthropic.md)에 Host가 연결 정보를 전달합니다. 실제 연결 검증은 로컬 계약 검증과 별도로 수행합니다.

```dotenv
ANTHROPIC_API_KEY=
ANTHROPIC_BASE_URL=https://api.anthropic.com
ANTHROPIC_API_VERSION=2023-06-01

ANTHROPIC_MODEL_1_ID=
ANTHROPIC_MODEL_1_EFFORT=
ANTHROPIC_MODEL_1_THINKING_MODE=
ANTHROPIC_MODEL_1_THINKING_BUDGET_TOKENS=
ANTHROPIC_MODEL_1_MAX_OUTPUT_TOKENS=4096

# 키가 특정 workspace에 한정되지 않은 경우에만 설정합니다.
# ANTHROPIC_WORKSPACE_ID=
```

1. [Claude Console의 API keys](https://platform.claude.com/settings/keys)에서 키를 생성하고 `ANTHROPIC_API_KEY`에 넣습니다. 개인 개발에는 개인 키, 공동 서비스에는 서비스 계정 키를 사용합니다. 키 생성 시 선택한 workspace 범위도 확인합니다. [키 생성·인증 방식](https://platform.claude.com/docs/en/manage-claude/authentication).
2. 특정 workspace에 한정되지 않은 키는 `ANTHROPIC_WORKSPACE_ID`도 필요합니다. Console의 **Settings → Workspaces**에서 ID를 확인합니다. 이 값은 HTTP의 `anthropic-workspace-id` 헤더에 대응합니다. [workspace 선택](https://platform.claude.com/docs/en/manage-claude/authentication#select-a-workspace).
3. Console에서 사용할 모델을 고른 뒤 [Models API](https://platform.claude.com/docs/en/api/models/list)의 목록과 [모델 ID·버전 문서](https://platform.claude.com/docs/en/about-claude/models/model-ids-and-versions)를 확인합니다. 직접 API의 정확한 ID를 `ANTHROPIC_MODEL_1_ID`에 복사합니다. Bedrock의 모델 ID나 화면 표시 이름을 넣지 않습니다.
4. 모델 식별자는 `ANTHROPIC_MODEL_1_ID` 하나만 입력합니다. 실제 release와 고정 여부는 해당 ID의 metadata를 검증해 내부 기록에 남깁니다. 별도 버전을 중복 입력하거나 이름에 날짜를 만들어 붙이지 않습니다. [ID와 버전의 의미](https://platform.claude.com/docs/en/about-claude/models/model-ids-and-versions).

모델 목록을 직접 조회하려면 키와 API 버전을 셸 환경변수에도 준비한 뒤 실행합니다. `.env` 파일을 저장하는 것만으로 셸에 변수가 설정되지는 않습니다. workspace가 필요한 키는 마지막 헤더도 추가합니다.

```sh
curl --fail-with-body --silent --show-error \
  'https://api.anthropic.com/v1/models' \
  -H "Authorization: Bearer $ANTHROPIC_API_KEY" \
  -H "anthropic-version: $ANTHROPIC_API_VERSION"
# workspace가 필요한 경우 위 명령에 추가:
# -H "anthropic-workspace-id: $ANTHROPIC_WORKSPACE_ID"
```

이 명령은 공식 모델 목록 조회이며 Wickle 연결 검사를 대신하지 않습니다. `has_more`가 참이면 `last_id`를 다음 요청의 `after_id`로 전달해 나머지 목록을 확인합니다. [Models API](https://platform.claude.com/docs/en/api/models/list).

## Effort와 thinking 설정

`ANTHROPIC_MODEL_1_EFFORT`는 논리 옵션 `effort`입니다. 직접 Messages API에서는 `output_config.effort`에 대응합니다. 지원 수준은 모델마다 다르므로 선택한 모델의 허용값을 확인해서 입력합니다. [Claude effort](https://platform.claude.com/docs/en/build-with-claude/effort).

`THINKING_MODE`는 `thinking_mode`, `THINKING_BUDGET_TOKENS`는 `thinking_budget_tokens`에 대응하는 선택 설정입니다. `adaptive`는 thinking 모드이며 effort 수준이 아닙니다. 수동 thinking을 지원하는 모델은 모드와 토큰 예산의 조합도 확인해야 합니다. [Adaptive thinking](https://platform.claude.com/docs/en/build-with-claude/adaptive-thinking), [수동 thinking 예산](https://platform.claude.com/docs/en/build-with-claude/extended-thinking).

옵션을 비워두면 전송하지 않습니다. 선택한 모델·API의 schema가 허용하지 않는 값이나 조합은 명시적인 오류로 처리하며, 조용히 무시하거나 다른 수준으로 바꾸지 않습니다. 논리 옵션을 실제 API 필드로 변환하는 부분은 해당 어댑터의 책임이며 Messages 어댑터가 매핑하며 실제 모델별 검증 결과는 별도로 확인해야 합니다.

`MAX_OUTPUT_TOKENS=4096`은 한 번의 모델 응답에 대한 출력 예산 예시입니다. 선택 모델의 요건과 thinking·도구 호출에 필요한 공간을 확인해 조정합니다. 모든 모델이나 긴 tool loop에 충분한 값이라는 뜻은 아닙니다.

같은 키·workspace에서 다른 모델을 추가하거나, 같은 모델 ID에 서로 다른 effort를 적용해 비교하려면 다음 슬롯을 사용합니다. 같은 모델을 비교할 때는 두 ID 칸에 같은 실제 ID를 넣고 effort에 각각 다른 허용값을 넣습니다.

```dotenv
ANTHROPIC_MODEL_2_ID=
ANTHROPIC_MODEL_2_EFFORT=
ANTHROPIC_MODEL_2_THINKING_MODE=
ANTHROPIC_MODEL_2_THINKING_BUDGET_TOKENS=
ANTHROPIC_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

번호는 `1`, `2`처럼 양의 정수이며, ID를 채운 각 번호가 독립된 모델·옵션 검사 대상입니다. 사용하지 않는 슬롯은 ID를 비워둡니다. 계정·workspace·endpoint가 다르면 별도의 `.env` 파일에 공통 설정과 모델 목록을 작성합니다.

`ANTHROPIC_API_VERSION`은 요청 헤더의 API 계약 버전입니다. 모델 release나 SDK 버전과는 별개입니다. [API 버전](https://platform.claude.com/docs/en/api/versioning).

## 확인한 현재 모델 계약

`claude-opus-5`는 thinking이 기본 활성화되어 있습니다. `EFFORT`는 `output_config.effort`로 전달하며 `adaptive`를 effort 값으로 넣지 않습니다. Thinking 블록이 첫 content일 수 있고 텍스트가 비어 있어도 signature를 포함해 후속 Tool 응답과 함께 보존해야 합니다. 높은 effort의 thinking도 출력 예산을 소비합니다. [Opus 5 변경사항](https://platform.claude.com/docs/en/models/opus-5/whats-new-opus-5), [effort](https://platform.claude.com/docs/en/build-with-claude/effort).
