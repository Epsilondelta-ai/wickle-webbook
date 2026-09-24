import {readFile,writeFile,mkdir,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const args=process.argv.slice(2),apply=args.includes('--apply');
const idx=args.indexOf('--project-ref'),ref=idx>=0?args[idx+1]:'';
if(ref&&!/^[a-z]{20}$/.test(ref))throw new Error('project-ref는 20자리 소문자 project ID여야 합니다.');
if(apply&&!ref)throw new Error('--apply에는 명시적인 --project-ref가 필요합니다.');
const m=JSON.parse(await readFile('content-manifest.json','utf8'));
for(const f of m.files){const b=await readFile('public/content/'+f.path);if(createHash('sha256').update(b).digest('hex')!==f.sha256)throw new Error('빌드 후 파일이 바뀌었습니다: '+f.path);}
console.log(JSON.stringify({mode:apply?'apply':'plan-only',project:ref||'not selected',bucket:'wickle-webbook',release:m.release,objects:m.files.length,publicRead:true,frontend:'GitHub Pages (separate)'},null,2));
if(!apply){console.log('No remote changes. Use --apply --project-ref <ref> after review.');process.exit(0);}
const dir=await mkdtemp(path.join(tmpdir(),'wickle-storage-'));await mkdir(path.join(dir,'supabase'));
await writeFile(path.join(dir,'supabase/config.toml'),'project_id = "wickle-webbook"\n[storage.buckets.wickle-webbook]\npublic = true\nfile_size_limit = "50MiB"\n');
const run=(a)=>{const r=spawnSync('supabase',a,{stdio:'inherit'});if(r.status!==0)throw new Error('Supabase CLI failed; incomplete release remains unpublished.');};
try{
 // Refuse to modify an existing bucket's configuration. Existing dedicated bucket can be reused.
 const check=spawnSync('supabase',['storage','ls','ss:///','--project-ref',ref,'--output','json'],{encoding:'utf8'});
 if(check.status!==0)throw new Error('Storage bucket 목록을 확인할 수 없습니다. CLI 로그인·권한을 확인하세요.');
 let buckets;try{buckets=JSON.parse(check.stdout);}catch{throw new Error('CLI bucket 목록 형식을 해석하지 못했습니다. 수동 검토가 필요합니다.');}
 const list=Array.isArray(buckets)?buckets:buckets.buckets;
 if(!Array.isArray(list))throw new Error('알 수 없는 CLI bucket 목록 형식입니다.');
 const existing=list.find(x=>x.name==='wickle-webbook'||x.id==='wickle-webbook');
 if(existing){if(existing.public!==true)throw new Error('기존 bucket의 public 속성을 확정할 수 없거나 비공개입니다. 설정을 자동 변경하지 않습니다.');}
 else run(['seed','buckets','--workdir',dir,'--project-ref',ref]);
 for(const f of [...m.files].sort((a,b)=>Number(a.path==='catalog.json')-Number(b.path==='catalog.json'))){
  run(['storage','cp',path.resolve('public/content',f.path),`ss:///wickle-webbook/${m.release}/${f.path}`,'--project-ref',ref,'--cache-control','max-age=31536000','--content-type',f.path.endsWith('.json')?'application/json':f.path.endsWith('.png')?'image/png':f.path.endsWith('.svg')?'image/svg+xml':f.path.endsWith('.zip')?'application/zip':'text/plain']);
 }
 console.log(`VITE_CONTENT_BASE_URL=https://${ref}.supabase.co/storage/v1/object/public/wickle-webbook/${m.release}/`);
}finally{await rm(dir,{recursive:true,force:true});}
