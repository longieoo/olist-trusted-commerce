"""Accept only evidence bound to this source tree and its current publications."""
import json
from pathlib import Path
from olist.contracts import utc_now
from olist.verification import code_hashes
from olist.local_warehouse import atomic_json,resolve_publication
from olist.io_utils import hash_file

code=code_hashes(); checks={}
for name in ['tests','integration-tests']:
    receipt=json.loads(Path(f'evidence/{name}.receipt.json').read_text())
    assert receipt['exit_code']==0 and receipt['code_unchanged']
    assert receipt['code_sha256']==code, f'Stale {name} receipt'
    assert receipt['xml_sha256']==hash_file(Path(f'evidence/{name}.xml'))
    checks[name]='passed on current source'
for filename,root in [('upgrade-corrections.json','data/simulation'),('upgrade-cdc.json','data/cdc'),
                      ('upgrade-dbt.json','data/local'),('final-dashboard.json','data/local')]:
    report=json.loads((Path('evidence')/filename).read_text())
    assert report['status']=='passed' and report['code_sha256']==code, f'Stale evidence: {filename}'
    assert report['generation']==resolve_publication(root)[1]['generation'], f'Stale generation: {filename}'
    checks[filename]={'status':'passed','checked_at':report['checked_at'],'generation':report['generation']}
source=json.loads(Path('evidence/final-source-oracle.json').read_text())
assert source['status']=='passed' and source['generation']==resolve_publication('data/local')[1]['generation']
checks['independent_csv_oracle']=source['per_order_mismatches']
result={'status':'passed','checked_at':utc_now(),'code_sha256':code,'checks':checks,
        'limits':['Single-host lab; no production HA or exactly-once claim',
        'Aggregate marts and full reference rebuild; publication copies full database',
        'CDC source synthetic; immutable dimensions and bounded journal (50000 records)',
        'Cloud deployment and hosted GitHub Actions not executed',
        'Airflow and legacy lifecycle replay evidence remains separately dated']}
atomic_json(Path('evidence/FINAL_VERIFICATION.json'),result)
print(json.dumps({k:v for k,v in result.items() if k!='code_sha256'},indent=2))
