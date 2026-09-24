# 웹북 검증 기록

기준: 2026-09-24 · Wickle 0.2.0 교재 · Node 20.19 · Chromium 자동화.

## 완료

- `npm run build`: 정적 사이트 빌드. 62개 강의·58개 구현 문서를 읽기 대상으로 생성.
- `npm test`: 계약 실습·콘텐츠 hash·공개 자료 범위 검사 9개 통과.
- `npm run test:browser`: 실제 Chromium 사용자 흐름 검사 6개 통과.
  - 홈 → 장 읽기 → 완료 → 새로고침 후 진도 유지 → 검색 성공/빈 결과 → 다음 장.
  - 옵션 병합·오류 입력, Presence 생략/null/모순, idle 취소와 이전 segment 결과 보존.
  - Rust 코드 편집과 main.rs 다운로드.
  - 390px 모바일 목차, 다크 테마 보존, 가로 overflow 없음.
  - 콘텐츠 요청 실패의 재시도.
  - `/book/` 하위 경로에서 직접 장 링크·새로고침·장 내 anchor·playground.
  - 주입된 HTML의 script/event handler/javascript URL 실행 차단.
- 홈·독서·playground·모바일 화면을 촬영하고 직접 시각 검토.
- npm audit: 취약점 0. 초기 DOMPurify 구버전 경고를 확인하고 3.4.16으로 고정 갱신.
- Supabase CLI 2.117.0의 init/help 및 변경 없는 배포 계획 출력 확인. 배포 명령·버킷 작업은 CLI 경로로 준비.

## 배포 상태와 한계

GitHub Pages/Supabase의 웹사이트 원격 배포는 수행하지 않았다. 사용자 요청에 따른 독립 GitHub 저장소 생성·push는 웹사이트 배포와 별도로 진행한다. 실제 GitHub Actions 실행과 Supabase bucket 생성·업로드는 사용자 프로젝트에서 후속 확인해야 한다. 로컬 `/book/` 배치 검사는 GitHub 원격 배포 자체의 검증이 아니다.

Rust 버튼이 외부 Playground URL에 코드를 전달하는 형식과 다운로드를 검증했다. 외부 Rust 서비스의 지속 가용성과 해당 사이트의 컴파일 결과는 보장하지 않는다. 브라우저 내 계약 playground는 교육용 JavaScript 구현이며 Rust Wickle 엔진의 실행 증거가 아니다.

기본 브라우저 실행 파일이 없어 첫 시도는 브라우저 시작 전에 실패했다. 현재 버전의 Chromium headless shell을 설치한 뒤 6개 사용자 흐름을 재실행해 통과했다.

스크린샷: `home-desktop.png`, `reader-desktop.png`, `playground-desktop.png`, `reader-mobile.png`. 자동 결과: `browser-results.json`, `npm-audit.json`.

## 독립 저장소 검증

스테이징된 전체 파일을 부모 교재 폴더가 없는 임시 디렉터리에 checkout하고 `npm ci`, `npm run build`, `npm test`를 실행해 통과했다. 생성된 content release hash도 원 작업 디렉터리와 같다. 자체 포함 교재는 `book-source/`에 저장한다.

공개 콘텐츠 검사에서 release의 `.env.example`까지 비밀 파일로 오분류한 초기 테스트를 수정했다. 실제 `.env` 및 그 변형은 제외하고 placeholder `.env.example`만 허용한다. 수정 후 9개 검사와 6개 브라우저 흐름 모두 통과했다. staged secret scan은 HIGH 0이며 MEDIUM은 코드 변수·타입, 합성 credential fixture, 예제 계정 숫자·timestamp, 로컬 테스트 URL과 binary patch encoding으로 확인했다.
