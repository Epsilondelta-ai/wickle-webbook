import {spawnSync} from 'node:child_process';
// Regenerate public/content from BOOK_SOURCE_DIR first. This imports only that filtered export.
const result=spawnSync('python3',['-c',`from pathlib import Path
import zipfile,shutil,tempfile
root=Path('book-source')
with tempfile.TemporaryDirectory(prefix='wickle-book-import-') as tmp:
 with zipfile.ZipFile('public/content/downloads/wickle-webbook-study.zip') as z:
  for name in z.namelist():
   p=Path(name)
   if p.is_absolute() or '..' in p.parts or p.parts[0]!='wickle-study':raise ValueError('Unsafe archive path')
   out=Path(tmp).joinpath(*p.parts[1:]);out.parent.mkdir(parents=True,exist_ok=True);out.write_bytes(z.read(name))
 if root.exists():shutil.rmtree(root)
 shutil.copytree(tmp,root)
print('Imported filtered book source; review changes before committing.')`],{stdio:'inherit'});
process.exit(result.status??1);
