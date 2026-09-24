import http from 'node:http';
import path from 'node:path';
import {readFile,stat} from 'node:fs/promises';
const root=path.resolve('dist'),prefix='/book/',port=4183;
const types={'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml','.zip':'application/zip','.png':'image/png'};
http.createServer(async(req,res)=>{try{const pathname=new URL(req.url,'http://localhost').pathname;if(!pathname.startsWith(prefix)){res.writeHead(404);res.end();return;}const relative=decodeURIComponent(pathname.slice(prefix.length))||'index.html';const file=path.resolve(root,relative);if(!file.startsWith(root+path.sep)){res.writeHead(403);res.end();return;}if(!(await stat(file)).isFile())throw Error();res.setHeader('Content-Type',types[path.extname(file)]||'text/plain');res.end(await readFile(file));}catch{res.writeHead(404);res.end('not found');}}).listen(port,'127.0.0.1',()=>console.log(`Subpath test server http://127.0.0.1:${port}${prefix}`));
