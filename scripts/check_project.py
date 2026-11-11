"""Offline integrity checks after directory cleanup; no service or secret required."""
import ast
import json
import re
from pathlib import Path
from urllib.parse import unquote

import duckdb
from olist.contracts import utc_now
from olist.fixtures import verified_manifest
from olist.local_warehouse import reconcile, resolve_publication
from olist.verification import code_hashes

root=Path(__file__).resolve().parents[1]
assert Path.cwd().resolve()==root, 'Run from the project root'
python_files=[p for folder in ['src','app','tests','scripts','dags'] for p in (root/folder).rglob('*.py')]
for p in python_files:
    ast.parse(p.read_text(encoding='utf-8'),filename=str(p))
broken=[]
documents=[root/'README.md',*(root/'docs').glob('*.md')]
for p in documents:
    for link in re.findall(r'\]\(([^)]+)\)',p.read_text(encoding='utf-8')):
        target=link.strip('<>').split('#',1)[0]
        if not target or re.match(r'^[a-z]+:',target,re.I): continue
        resolved=(p.parent/unquote(target)).resolve()
        if resolved==root/'evidence/CLEANUP_VERIFICATION.json': continue  # This invocation produces it.
        if not resolved.exists(): broken.append([p.name,target])
assert not broken, f'Broken local documentation links: {broken}'
datasets={}
for name in ['real','fixtures']:
    manifest=verified_manifest(root/'data'/name)
    datasets[name]={'dataset_version':manifest['dataset_version'],'tables':len(manifest['files']),
                    'records':sum(entry['records'] for entry in manifest['files'])}
publications={}
for name in ['local','local-fixture']:
    path,receipt=resolve_publication(root/'data'/name)
    with duckdb.connect(str(path),read_only=True) as conn:
        checks=reconcile(conn)
        count=conn.execute('SELECT count(*) FROM fact_orders').fetchone()[0]
    publications[name]={'generation':receipt['generation'],'orders':count,'checks':checks}
for old in ['powerbi','evidence-imported','dist']:
    assert not (root/old).exists(), f'Superseded directory remains: {old}'
result={'checked_at':utc_now(),'status':'passed','python_files':len(python_files),
        'documents_with_valid_links':len(documents),'datasets':datasets,'publications':publications,
        'code_sha256':code_hashes(),
        'scope':'Offline source syntax, local documentation links, original CSV checksums, published database checksums and reconciliation',
        'docker':'Not tested by this offline command; consult upgrade-cdc.json and integration-tests.receipt.json for runtime evidence'}
(root/'evidence/CLEANUP_VERIFICATION.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
# Offline checks must never overwrite the broader runtime acceptance report.
print(json.dumps({k:v for k,v in result.items() if k!='code_sha256'},indent=2))
