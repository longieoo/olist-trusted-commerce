"""Execute actual dbt against a disposable publication copy, then compare all models."""
import argparse
import json
import os
import shutil
import subprocess
from collections import Counter
from pathlib import Path
import duckdb
from olist.contracts import utc_now
from olist.models import export_dbt,definitions
from olist.local_warehouse import resolve_publication,atomic_json
from olist.verification import code_hashes

p=argparse.ArgumentParser(); p.add_argument('--dbt',default='dbt'); p.add_argument('--source-root',default='data/local'); a=p.parse_args()
project=Path(export_dbt('data/runtime/dbt-local')).resolve()
source,receipt=resolve_publication(a.source_root)
target=project/'warehouse.duckdb'; shutil.copyfile(source,target)
env={**os.environ,'OLIST_DBT_DATABASE':str(target),'DBT_SEND_ANONYMOUS_USAGE_STATS':'false'}
run=subprocess.run([a.dbt,'build','--project-dir',str(project),'--profiles-dir',str(project)],
                   env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace')
(project/'build.log').write_text(run.stdout,encoding='utf-8')
print(run.stdout)
if run.returncode: raise SystemExit(run.returncode)
def rows(db,name):
    return Counter(json.dumps([round(v,8) if isinstance(v,float) else v for v in row],default=str)
                   for row in db.execute('SELECT * FROM '+name).fetchall())
with duckdb.connect(str(target),read_only=True) as db:
    parity={}
    for name,_ in definitions():
        left,right=rows(db,'main.'+name),rows(db,'main_analytics.'+name)
        parity[name]=sum((left-right).values())+sum((right-left).values())
assert not any(parity.values()),parity
results=json.loads((project/'target/run_results.json').read_text())
assert all(r['status'] in ('success','pass') for r in results['results'])
report={'status':'passed','checked_at':utc_now(),'generation':receipt['generation'],
        'dbt_version':results['metadata']['dbt_version'],'results':len(results['results']),
        'model_parity':parity,'canonical_sql':'sql/local_marts.sql',
        'scope':'dbt build on isolated copy; generated models, no second hand-maintained metric definition',
        'code_sha256':code_hashes()}
atomic_json(Path('evidence/upgrade-dbt.json'),report)
print(json.dumps({k:v for k,v in report.items() if k!='code_sha256'},indent=2))
