"""Package the current source and fresh evidence without secrets/runtime databases."""
import hashlib
import json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root=Path(__file__).resolve().parents[1]
target=root/'dist'/'Olist_Final.zip';target.parent.mkdir(exist_ok=True)
excluded={'.git','.venv','.pytest_cache','__pycache__','dist','evidence-imported','powerbi'}
files=[]
with ZipFile(target,'w',ZIP_DEFLATED,compresslevel=6) as archive:
    for path in sorted(root.rglob('*')):
        if not path.is_file():continue
        rel=path.relative_to(root)
        if any(p in excluded or p.endswith('.egg-info') for p in rel.parts):continue
        if rel.parts[:2] in [('data','local'),('data','local-fixture'),('data','runtime'),('data','simulation'),('data','cdc'),('dbt','logs'),('dbt','target')]:continue
        if path.name in {'.env','profiles.yml','.user.yml'} or path.suffix in {'.pyc','.docx','.pbix','.duckdb','.wal'}:continue
        archive.write(path,Path('olist-final')/rel)
        files.append({'path':rel.as_posix(),'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    archive.writestr('olist-final/PACKAGE_CONTENTS.json',json.dumps(files,indent=2))
with ZipFile(target) as archive:
    if archive.testzip() is not None:raise AssertionError('Corrupt archive')
    if any(Path(n).name=='.env' for n in archive.namelist()):raise AssertionError('Secret environment included')
report={'archive':str(target),'files':len(files),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
        'excluded':'secrets, runtime/checkpoints, local databases, historical evidence, legacy Word/Power BI artifacts',
        'data':'Includes user-provided real CSVs for local handoff; not a public redistribution license.'}
target.with_suffix('.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report,indent=2))
