import {test,expect} from '@playwright/test';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';

test('revisiting a chapter revalidates cached content after publication',async({page})=>{
 const catalog=JSON.parse(await readFile('dist/content/catalog.json','utf8'));
 const chapter=catalog.documents.find(d=>d.path==='ko/02b-architecture.md');
 const target='/content/docs/'+chapter.id+'.json';
 const current=JSON.parse(await readFile('dist'+target,'utf8'));
 let published=false;
 const server=createServer(async(req,res)=>{
  const pathname=new URL(req.url,'http://localhost').pathname;
  if(pathname===target){
   res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'max-age=600'});
   res.end(JSON.stringify(published?current:{...current,body:'# 이전 교재\n\n```text\nRunning --> Waiting\n```'}));return;
  }
  try{
   const file=pathname==='/'?'/index.html':pathname;
   const body=await readFile(path.join(process.cwd(),'dist',file));
   const types={'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml'};
   res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream'});res.end(body);
  }catch{res.writeHead(404);res.end();}
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 try{
  const base='http://127.0.0.1:'+server.address().port+'/';
  await page.goto(base+'#/read/ko%2F02b-architecture.md');
  await expect(page.locator('#article h1')).toHaveText('이전 교재');
  published=true;
  await page.getByRole('link',{name:'학습 안내',exact:false}).first().click();
  await page.getByRole('link',{name:'아키텍처 강의 읽기',exact:false}).click();
  await expect(page.locator('.book-diagram')).toHaveCount(5);
  for(const img of await page.locator('.book-diagram img').all())await expect.poll(()=>img.evaluate(e=>e.complete&&e.naturalWidth>0)).toBe(true);
 }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});
