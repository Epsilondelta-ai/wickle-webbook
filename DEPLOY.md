# 웹북 배포

## 기본 배포: GitHub Pages

이 웹북에는 서버나 Supabase 프로젝트가 필수로 필요하지 않습니다. 정적 HTML/CSS/JS와 교재 JSON을 제공합니다. 진도와 테마는 브라우저 localStorage에 저장됩니다. 다른 기기·브라우저와 동기화되지 않습니다.

### 이미 빌드한 배포 패키지

`release/wickle-webbook-github-pages.zip`을 새 폴더에 풉니다. 다음을 웹북 전용 저장소 **루트**에 넣습니다.

```text
.github/workflows/pages.yml
site/index.html
site/assets/
site/content/
README.md
DEPLOY.md
content-manifest.json
```

1. GitHub 저장소 Settings → Pages → Build and deployment → Source를 **GitHub Actions**로 선택합니다.
2. `.github/`까지 포함해 파일을 저장소에 올립니다. Wickle 엔진 저장소 대신 별도 웹북 저장소를 사용하는 것을 권장합니다.
3. Actions → **Publish Wickle Webbook** → **Run workflow**를 선택합니다.
4. 배포 job의 `github-pages` URL에서 확인합니다.

워크플로는 `workflow_dispatch`만 사용합니다. 파일 업로드나 push만으로 자동 공개 배포되지 않습니다. Pages가 제공 가능한 계정·저장소 유형인지 확인하세요. 회사 내부 전용 접근통제는 이 정적 사이트에 구현하지 않았습니다.

Vite의 `base: './'` 및 `#/read/...` 경로를 사용하므로 `https://계정.github.io/저장소명/` 하위에서도 assets 경로와 직접 링크·새로고침이 동작합니다. 주소창의 hash는 제거하지 않습니다. GitHub Pages는 Rust compiler나 동적 서버를 실행하지 않습니다.

### 콘텐츠 변경 후 다시 만들기

이 웹북 저장소 루트에서 실행합니다. 교재 원본 `book-source/`가 포함돼 다른 프로젝트나 부모 디렉터리가 필요하지 않습니다.

```sh
npm ci
npm run build
npm test
npm run test:browser
npm run package:pages
```

새 배포 폴더의 `site/`를 교체하고 수동 workflow를 실행합니다. 구 `site/content/`도 새 폴더로 교체해 오래된 콘텐츠가 남지 않도록 합니다. Git history에는 삭제 이력이 남습니다. 원래 Markdown 원본을 직접 수정하고 rebuild하면 됩니다.

## Supabase를 나중에 콘텐츠 CDN으로 사용할 경우

웹 화면은 GitHub Pages에 두고 교재 JSON·파일만 Supabase Storage에 둘 수 있습니다. **공식 문서상 Supabase Storage의 HTML은 plain text로 제공되며, custom domain도 frontend hosting 용도가 아닙니다.** 웹 화면을 Storage에 올리면 웹사이트로 열릴 것으로 기대하면 안 됩니다.

이 소스에는 Supabase CLI 2.117.0의 `--help`로 확인한 선택적 업로드 도구가 있습니다. 기본은 변경 없는 계획 출력입니다. DB 테이블·Auth·RLS SQL·Edge Function을 만들 필요가 없습니다.

```sh
supabase login
supabase projects list
npm run deploy:plan -- --project-ref YOUR_PROJECT_REF
# 계획과 공개할 자료를 검토한 후에만:
npm run deploy:storage -- --project-ref YOUR_PROJECT_REF
```

apply는 `supabase seed buckets --project-ref ...`로 전용 공개 bucket을 만들고, `supabase storage cp --project-ref ...`로 content만 업로드합니다. 임의 프로젝트 자동 선택, 기존 bucket 변경, DB migration, config push, 프로젝트 생성은 수행하지 않습니다. 업로드는 해당 프로젝트에 대한 CLI 로그인 권한을 사용하며 브라우저에 service-role key를 넣지 않습니다.

스크립트가 출력한 immutable content prefix URL을 `.env.local`의 `VITE_CONTENT_BASE_URL`에 설정하고 다시 빌드합니다. secret이 아니라 공개 콘텐츠 URL만 넣습니다.

```text
VITE_CONTENT_BASE_URL=https://PROJECT.supabase.co/storage/v1/object/public/wickle-webbook/RELEASE/
```

모든 파일 업로드가 끝난 뒤 catalog를 마지막에 업로드합니다. 다른 release prefix에 올리므로 이전 배포가 중간에 손상되지 않습니다. 인터넷/CORS/공개 read 접근을 실제 프로젝트에서 검증해야 하며, 이번 제작 과정에서는 원격 프로젝트를 수정하지 않았습니다.

## playground 실행 범위

- 옵션 계층·Presence·segment 실험: 브라우저 JavaScript 기반 교육 모델. 실제 Wickle API·DB·권한 집행이 아닙니다.
- Rust 코드: 편집·파일 다운로드 가능. 실행 버튼은 코드가 URL에 포함된 공식 `play.rust-lang.org` 새 탭을 엽니다. 그 탭에서 Run으로 실제 컴파일합니다.
- 외부로 보내기 전에 UI에 고지합니다. 비밀키·개인정보를 입력하지 마세요. 긴 코드는 다운로드해 로컬 Cargo에서 실행합니다.
- Wickle 전체와 Tokio·SQLite·공급자 adapter는 로컬 교재 실습을 따릅니다. GitHub Pages/Supabase에서 임의 Rust를 직접 실행하지 않습니다.

## 공개 콘텐츠의 범위

62개 강의, 58개 전체 구현 문서, 참고 가이드, 0.1.0/0.2.0 reference 소스와 patch를 웹에서 읽거나 내려받습니다. 내부 설계 원문 사본, 작업 검증 로그, 로컬 계획 파일, 인증정보는 공개 콘텐츠 목록에서 제외합니다. 제외된 로컬 문서 링크는 실행되지 않는 ‘로컬 교재’ 표기로 보여 줍니다. 공개 업로드의 전체 파일·hash는 `content-manifest.json`으로 검토할 수 있습니다.

## 근거

- [GitHub Pages custom workflow](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
- [Vite 정적 배포](https://vite.dev/guide/static-deploy.html)
- [Supabase custom domain 제약](https://supabase.com/docs/guides/platform/custom-domains)
- [Supabase Storage quickstart](https://supabase.com/docs/guides/storage/quickstart)
- [Rust Playground 원본 프로젝트](https://github.com/rust-lang/rust-playground)

문서 확인일: 2026-09-24. 실행한 로컬 검증과 원격 미배포 상태는 `verification/RESULTS.md`에 기록합니다.

## 소스 저장소에서 직접 배포

이 저장소 자체에도 `.github/workflows/pages.yml`이 있습니다. 이 workflow는 `npm ci` → 콘텐츠·정적 빌드 → unit test → Pages artifact 업로드를 수행하며, 수동 실행만 허용합니다. 별도의 prebuilt zip을 쓰지 않아도 됩니다. 조직 Free 요금제의 private repository에는 Pages 제약이 있으므로 공개 전환 또는 지원 요금제 결정 후 활성화합니다. 저장소 생성·push는 사이트 공개와 다른 작업입니다.
