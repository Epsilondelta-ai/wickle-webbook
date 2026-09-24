import { readdir, readFile, writeFile, mkdir, cp, rm } from 'node:fs/promises';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import { createHash } from 'node:crypto';
const root = path.resolve(process.env.BOOK_SOURCE_DIR || 'book-source');
const output = path.resolve('public/content');
const hash = value => createHash('sha256').update(value).digest('hex');
// Publish only teaching documents and release references, never private design records/logs.
const excluded = new Set(['design-reading.md', 'design-sources.md', 'VALIDATION-v0.1-baseline.md']);
const files = [];
async function walk(dir, prefix) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    if (['target', '.git', '__pycache__'].includes(entry.name)) continue;
    if(entry.name.startsWith('.env') && entry.name !== '.env.example') continue;
    const rel = `${prefix}/${entry.name}`;
    if (entry.isDirectory()) await walk(path.join(dir, entry.name), rel);
    else if (entry.isFile()) files.push(rel);
  }
}
await walk(path.join(root, 'ko'), 'ko');
await walk(path.join(root, 'reference'), 'reference');
await walk(path.join(root, 'reference-v0.1.0'), 'reference-v0.1.0');
await rm(output, { recursive: true, force: true });
await mkdir(path.join(output, 'docs'), { recursive: true });
await mkdir(path.join(output, 'files'), { recursive: true });
const documents = [], assets = {};
for (const rel of files.sort()) {
  if (rel.startsWith('ko/') && excluded.has(path.basename(rel))) continue;
  if (rel.startsWith('ko/') && /\/(?:\.gitattributes|\.gitignore)$/.test(rel)) continue;
  const bytes = await readFile(path.join(root, rel));
  const ext = path.extname(rel);
  if (['.md', '.rs', '.toml', '.py'].includes(ext) || path.basename(rel) === 'LICENSE') {
    let body = bytes.toString('utf8');
    if (ext === '.md') {
      // Internal planning references are deliberately not published.
      body = body.replace(/\[([^\]]+)\]\(((?:\.\.\/){3,4})(?:v0\.2\.0|agent-core[^/]*|verification)\/[^)]+\)/g, '$1 (로컬 교재의 참고 기록)');
      body = body.replace(/\[([^\]]+)\]\((?:design-reading|design-sources|VALIDATION-v0\.1-baseline)\.md\)/g, '$1 (로컬 교재 참고)');
      body = body.replace(/\[([^\]]+)\]\(\.\.\/verification\/[^)]+\)/g, '$1 (로컬 검증 기록)');
    }
    const isChapter = /^ko\/\d\d[b]?-/.test(rel);
    const title = ext === '.md' ? (body.match(/^#\s+(.+)$/m)?.[1] || path.basename(rel)) : rel;
    const id = hash(rel).slice(0, 20);
    const kind = isChapter ? 'chapter' : rel.startsWith('ko/implementation/') ? 'implementation' : rel.startsWith('ko/') ? 'guide' : 'source';
    const language = ({ '.rs': 'rust', '.toml': 'toml', '.py': 'python' })[ext];
    const payload = { path: rel, title, body, markdown: ext === '.md', language };
    await writeFile(path.join(output, 'docs', id + '.json'), JSON.stringify(payload));
    const plain = body.replace(/```[\s\S]*?```/g, ' ').replace(/[#*`>|[\]]/g, ' ').replace(/\s+/g, ' ');
    const rawPath='files/'+id+'-'+path.basename(rel);
    await writeFile(path.join(output,rawPath),body); assets[rel]=rawPath;
    documents.push({ path: rel, id, title, kind, number: isChapter ? rel.slice(3, 6).replace(/-$/, '') : '', excerpt: plain.slice(0, 180), search: kind === 'chapter' || kind === 'guide' ? plain : '', minutes: Math.max(3, Math.ceil(body.length / 1100)) });
  } else if (rel.startsWith('reference') || ext === '.patch' || ['.json', '.lock', '.png', '.svg'].includes(ext) || rel === 'ko/lab.py') {
    const dest = 'files/' + hash(rel).slice(0, 16) + '-' + path.basename(rel);
    await cp(path.join(root, rel), path.join(output, dest)); assets[rel] = dest;
  }
}
// The public code bundle excludes all internal planning, logs, and local paths.
await writeFile(path.join(output, 'catalog.json'), JSON.stringify({ version: '0.2.0', commit: 'b1416772dd185a3b175db6e3099512d9006f48f5', documents, assets }));
await mkdir(path.join(output,'downloads'),{recursive:true});
const zip=spawnSync('python3',['-c',`from pathlib import Path
import json,zipfile
root=Path('public/content');c=json.loads((root/'catalog.json').read_text())
with zipfile.ZipFile(root/'downloads/wickle-webbook-study.zip','w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
 for original,p in c['assets'].items():
  info=zipfile.ZipInfo('wickle-study/'+original,(2026,9,24,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED;z.writestr(info,(root/p).read_bytes())
 info=zipfile.ZipInfo('wickle-study/README.md',(2026,9,24,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
 z.writestr(info,'# Wickle 0.2.0 학습자료\\n\\nko/README.md에서 시작하세요. 내부 설계·검증 원문은 웹 공개본에서 제외했습니다. 링크의 로컬 기록 표기는 원 교재를 뜻합니다.\\n')
`],{stdio:'inherit'});
if(zip.status)throw new Error('Study archive generation failed');
const published = [];
async function collect(dir) { for (const e of await readdir(dir,{withFileTypes:true})) { const p=path.join(dir,e.name); if(e.isDirectory()) await collect(p); else published.push({path:path.relative(output,p).split(path.sep).join('/'),sha256:hash(await readFile(p))}); } }
await collect(output);
const release = 'v0.2.0-' + hash(JSON.stringify(published)).slice(0,12);
await writeFile('content-manifest.json', JSON.stringify({ release, files: published, documents: documents.length, chapters: documents.filter(d=>d.kind==='chapter').length },null,2)+'\n');
console.log(`${release}: ${documents.length} readable documents, ${documents.filter(d=>d.kind==='chapter').length} chapters, ${published.length} assets`);
