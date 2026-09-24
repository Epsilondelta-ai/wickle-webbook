# xAI Grok 환경 설정

xAI 직접 API의 인증 정보와 모델별 테스트 대상을 준비합니다. 키와 endpoint는 공통으로 한 번 설정하고, 테스트할 모델이나 릴리스마다 슬롯을 추가합니다.

## 콘솔에서 확인할 값

1. [xAI Console](https://console.x.ai/)에 로그인해 API를 사용할 계정·팀을 선택합니다. API 사용에 필요한 크레딧을 확인한 뒤 **API Keys**에서 키를 만들고 `XAI_API_KEY`에 넣습니다. [공식 시작 안내](https://docs.x.ai/developers/quickstart)
2. Console에서 사용할 수 있는 모델과 [공식 모델 목록](https://docs.x.ai/developers/models)을 대조하고 정확한 API model ID를 복사합니다. 공개 문서에 있는 모델이라도 해당 계정·키에서 사용할 수 있는지 따로 확인합니다.
3. 버전별 테스트에는 공식 모델 페이지에서 확인한 릴리스 ID를 사용합니다. xAI는 모델 기본 이름이나 `-latest` 이름이 바뀔 수 있는 alias이고, 날짜가 포함된 릴리스 ID는 특정 릴리스를 가리킬 수 있다고 안내합니다. 이름에 임의로 날짜를 붙이지 말고 공개된 정확한 ID를 복사하세요. [xAI 모델 alias와 릴리스](https://docs.x.ai/developers/models#model-aliases)

## 복사할 `.env` 블록

```dotenv
# 공통 인증과 endpoint
XAI_API_KEY=
XAI_BASE_URL=https://api.x.ai/v1

# 첫 번째 모델: 실제 사용 가능한 호출 ID
XAI_MODEL_1_ID=
XAI_MODEL_1_REASONING_EFFORT=
XAI_MODEL_1_MAX_OUTPUT_TOKENS=4096

# 두 번째 모델 또는 같은 모델의 다른 릴리스
XAI_MODEL_2_ID=
XAI_MODEL_2_REASONING_EFFORT=
XAI_MODEL_2_MAX_OUTPUT_TOKENS=4096
```

| 설정 | 입력할 값 |
| --- | --- |
| `XAI_API_KEY` | 선택한 xAI 계정·팀에서 만든 API key |
| `XAI_BASE_URL` | `/v1`을 포함한 xAI API 주소 |
| `XAI_MODEL_1_ID` | 실제 요청의 `model`에 전달할 정확한 모델 ID |
| `XAI_MODEL_1_REASONING_EFFORT` | 선택한 모델·API에서 허용하는 reasoning effort |
| `XAI_MODEL_1_MAX_OUTPUT_TOKENS` | 한 번의 모델 응답에 허용할 출력 토큰 수 |

모델 식별자는 `_ID` 하나만 입력합니다. 실제 release와 alias의 고정 여부는 검증 단계에서 확인하여 내부 metadata에 기록합니다. `/v1`은 API 경로 버전이며 모델 릴리스와 별개입니다.

## Reasoning effort 설정

`XAI_MODEL_1_REASONING_EFFORT`는 논리 옵션 `reasoning_effort`입니다. 현재 공식 Responses API 예제에서는 `reasoning.effort`에 대응합니다. 지원 여부와 허용 수준은 모델·API별로 다르므로 선택한 조합의 schema를 확인합니다. 구형 모델의 제한을 모든 Grok 모델에 적용하지 않습니다. [xAI reasoning](https://docs.x.ai/developers/model-capabilities/text/reasoning).

빈 effort는 전송하지 않습니다. 지원하지 않는 값이 공급자에서 무시·대체될 수 있더라도 테스트에서는 명시적으로 거부하며 조용히 낮은 수준으로 바꾸지 않습니다. [xAI 어댑터](../xai.md)가 논리 옵션을 명시적 Responses 요청으로 변환합니다. 실제 계정 연결 검증은 별도로 진행합니다.

`MAX_OUTPUT_TOKENS=4096`은 응답 한 번의 출력 예산 예시입니다. 모델의 요건과 reasoning·도구 호출에 필요한 토큰에 맞춰 조정합니다. 긴 tool loop 전체에 충분한 예산이라는 뜻은 아닙니다.

두 모델은 각각 `_1_ID`와 `_2_ID`에 넣습니다. 같은 모델에 다른 effort를 비교하려면 두 ID를 동일하게 넣고 각 슬롯의 `REASONING_EFFORT`에 다른 허용값을 입력합니다. 더 필요하면 `XAI_MODEL_3_ID`, `XAI_MODEL_3_REASONING_EFFORT`, `XAI_MODEL_3_MAX_OUTPUT_TOKENS`처럼 양의 정수 번호를 늘립니다.

ID가 채워진 슬롯마다 독립적인 모델·옵션 테스트 대상이 됩니다. 사용하지 않는 슬롯은 ID를 비워둡니다. 별도 계정이나 endpoint를 사용할 때는 별도 `.env` 파일로 준비합니다.

이 문서는 모델별 실제 연결 테스트용 설정 규약입니다. `.env` 저장만으로 호출이 실행되지 않으며 코어 라이브러리가 파일을 직접 읽지 않습니다. 준비 및 실행 범위는 [공통 설정 안내](README.md)를 따릅니다.

## 확인한 현재 모델 계약

`grok-4.6`은 Responses의 `reasoning.effort`에 `low`, `medium`, `high`, `xhigh`를 지원하며 기본은 `high`입니다. Thinking 비활성화는 지원하지 않습니다. `xhigh`를 지원하지 않는 구형 모델에 동일 옵션을 보내 조용히 강등시키지 않습니다. [Grok 4.6](https://docs.x.ai/developers/models/grok-4.6), [reasoning](https://docs.x.ai/developers/model-capabilities/text/reasoning).

일부 retired slug는 공급자 정책에 따라 다른 모델로 자동 대체됩니다. 이름이나 날짜만으로 immutable release라고 판정하지 않고, 실제 응답 모델과 별도 release 증거를 확인합니다. [공식 이전 정책](https://docs.x.ai/developers/migration/may-15-retirement).
