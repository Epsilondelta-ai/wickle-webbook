# Wickle Webbook 0.2.0

독립 저장소: [Epsilondelta-ai/wickle-webbook](https://github.com/Epsilondelta-ai/wickle-webbook)

한국어 학습자료를 읽고 실험하는 정적 웹북입니다. **GitHub Pages 배포가 기본**이며 Supabase는 필요할 때 교재 콘텐츠 저장소로만 연결합니다. 웹사이트 공개 배포는 별도의 수동 작업입니다.

## 실행

Node 20.19 이상, npm, Python 3가 필요합니다. 공개용 교재 원본은 저장소의 `book-source/`에 포함되어 있어 이 저장소만 clone하면 됩니다.

```sh
npm ci
npm run dev
```

브라우저에서 `http://127.0.0.1:5173`을 엽니다. `file://`로 index.html을 더블클릭하면 JSON 로딩이 제한될 수 있으므로 HTTP 서버로 실행합니다.

## 기능

- 62개 강의, 58개 전체 구현 문서, 버전별 참조 코드 읽기.
- 강의 제목·본문 검색, 코드 파일 이름 검색, 단축키 Cmd/Ctrl+K.
- 그룹별 목차, 장 내 목차, 이전/다음 장, hash 기반 직접 링크.
- 코드 강조·복사·원문 내려받기, 교재·코드 ZIP 다운로드.
- 학습 완료 표시와 이어 읽기, 브라우저 내 진도·테마 보존.
- 모바일 목차와 다크 모드, 키보드 포커스와 인쇄용 스타일.
- 옵션 우선순위·Presence 복원·실행 segment 전이 실험.
- Rust 코드 편집·main.rs 다운로드·공식 Rust Playground 연결.

Playground의 세 계약 실습은 JavaScript 교육 모델입니다. 실제 Wickle runtime·SQLite·권한을 실행하지 않습니다. Rust 버튼은 코드를 포함한 URL로 공식 Rust Playground를 열며, 해당 사이트에서 Run을 눌러 컴파일합니다. `wickle` 전체 dependency를 임의의 웹 서버에서 실행하는 기능은 아닙니다.

## 빌드·검증·배포 패키지

```sh
npm run build
npm test
npx playwright install chromium --only-shell
npm run test:browser
npm run package:pages
```

- `dist/`: 어느 정적 호스트에도 올릴 수 있는 완성 웹사이트.
- `release/github-pages/`: `site/`와 수동 Actions workflow를 가진 전용 저장소용 폴더.
- `release/wickle-webbook-github-pages.zip`: 위 폴더의 압축본.
- `release/SHA256SUMS`: 배포 압축본 checksum.
- [DEPLOY.md](DEPLOY.md): GitHub Pages와 선택적인 Supabase CLI 업로드 절차.
- [검증 기록](verification/RESULTS.md): 실제 실행 범위와 미검증 범위.

`npm run package:pages`는 로컬 파일만 만듭니다. 외부 서비스에 게시하지 않습니다. 워크플로는 `workflow_dispatch`만 사용하므로 실제 배포는 사용자가 명시적으로 실행합니다.

## 구조

```text
book-source/               자체 포함된 교재·참조 코드·patch
scripts/build-content.mjs   원본 교재에서 공개할 콘텐츠를 생성
src/main.js                읽기·검색·진도·playground UI
src/labs.js                테스트 가능한 개념 실험 로직
src/style.css              반응형 읽기 화면
public/content/            생성된 문서·다운로드 자료 (직접 편집 금지)
scripts/package-pages.mjs  GitHub Pages 배포 폴더와 ZIP 생성
scripts/deploy-storage.mjs 선택적 Supabase CLI 업로드, 기본 dry-run
```

`book-source/ko/`의 교재 Markdown을 고친 후 rebuild하면 반영됩니다. 원래 교재에서 다시 가져오려면 `BOOK_SOURCE_DIR=/교재/v0.2.0 npm run content`로 공개본을 생성한 뒤 `npm run import:book`으로 반영합니다. 버전별 원문 코드는 그대로 두고 웹 표현만 생성합니다. 큰 전체 구현 문서는 선택한 페이지를 열 때만 가져오며 긴 코드 블록은 강조를 생략하고 원문을 유지합니다. 초기 검색 index는 강의·가이드 본문과 파일 metadata만 포함합니다.

Google Fonts를 사용하며 연결이 안 되면 로컬 serif/sans-serif/monospace로 표시합니다. 계정 로그인·서버 진도 동기화는 없습니다. public 콘텐츠에 내부 설계 사본·검증 로그를 포함하지 않으며, 제외된 참고 링크는 ‘로컬 교재’로 표시합니다.

## Supabase (선택)

`VITE_CONTENT_BASE_URL`이 비어 있으면 배포에 모든 교재 자료가 포함됩니다. Supabase CLI로 공개 Storage bucket에 올린 뒤 immutable release URL을 설정하면 같은 프런트엔드가 원격 콘텐츠를 읽습니다. 서버 비밀키를 클라이언트에 넣지 않습니다. 자세한 절차는 DEPLOY.md를 따릅니다.
