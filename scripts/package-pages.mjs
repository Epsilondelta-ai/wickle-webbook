import { mkdir, cp, writeFile, rm, readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
const out='release/github-pages';
await rm(out,{recursive:true,force:true});
await mkdir(out+'/.github/workflows',{recursive:true});
await cp('dist',out+'/site',{recursive:true});
await writeFile(out+'/site/.nojekyll','');
const workflow=`name: Publish Wickle Webbook
on:
  workflow_dispatch:
permissions:
  contents: read
  pages: write
  id-token: write
concurrency:
  group: pages
  cancel-in-progress: false
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: \${{ steps.deployment.outputs.page_url }}
    steps:
      - uses: actions/checkout@v6
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v4
        with:
          path: site
      - name: Deploy
        id: deployment
        uses: actions/deploy-pages@v4
`;
await writeFile(out+'/.github/workflows/pages.yml',workflow);
await cp('DEPLOY.md',out+'/DEPLOY.md');
await cp('content-manifest.json',out+'/content-manifest.json');
await writeFile(out+'/README.md','# Wickle Webbook 0.2.0\n\n`site/`는 이미 빌드된 정적 웹북입니다. 별도 Node 빌드나 Supabase key 없이 동작합니다.\n\n1. 이 폴더의 내용을 웹북 전용 GitHub 저장소 루트에 넣습니다. `.github/`도 포함합니다.\n2. Settings → Pages → Source를 GitHub Actions로 지정합니다.\n3. Actions → Publish Wickle Webbook → Run workflow를 수동 실행합니다.\n4. 배포가 끝난 뒤 workflow의 github-pages URL을 엽니다.\n\n이 패키지를 만드는 작업 자체는 저장소 생성·push·공개 배포를 수행하지 않습니다. 배포 시 `site/`의 모든 콘텐츠가 공개됩니다.\n\n`base: ./`와 hash 경로를 사용하므로 `/repository-name/` 아래에서도 새로고침과 링크가 동작합니다.\n');
const result=spawnSync('python3',['-c',`from pathlib import Path\nimport zipfile,hashlib\nroot=Path('release/github-pages')\narchive=Path('release/wickle-webbook-github-pages.zip')\nwith zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:\n for p in sorted(root.rglob('*')):\n  if p.is_file():z.write(p,p.relative_to(root).as_posix())\nwith zipfile.ZipFile(archive) as z:assert z.testzip() is None\nPath('release/SHA256SUMS').write_text(hashlib.sha256(archive.read_bytes()).hexdigest()+'  '+archive.name+'\\n')\nprint(str(archive),archive.stat().st_size,'bytes')`],{stdio:'inherit'});
if(result.status)process.exit(result.status);
console.log('Prepared only. No GitHub resources created or deployed.');
