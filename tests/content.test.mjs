import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const manifest=JSON.parse(await readFile('content-manifest.json','utf8'));
const catalog=JSON.parse(await readFile('public/content/catalog.json','utf8'));
test('all 62 chapters and 58 implementations are available',()=>{assert.equal(catalog.documents.filter(d=>d.kind==='chapter').length,62);assert.equal(catalog.documents.filter(d=>d.kind==='implementation').length,58);assert(catalog.documents.find(d=>d.path==='ko/60-release-v02.md'));assert.equal(new Set(catalog.documents.map(d=>d.path)).size,catalog.documents.length);});
test('build manifest hashes every published object',async()=>{for(const f of manifest.files){const b=await readFile('public/content/'+f.path);assert.equal(createHash('sha256').update(b).digest('hex'),f.sha256,f.path);}});
test('internal raw archives and execution logs are not published',()=>{const paths=[...catalog.documents.map(d=>d.path),...Object.keys(catalog.assets)];assert(!paths.some(p=>/verification\/|design-reading|design-sources|\.env(?!\.example$)/.test(p)));});
