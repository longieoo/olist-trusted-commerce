"""Assemble fresh verification evidence; fail closed on missing checks."""
import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from olist.local_warehouse import resolve_publication
from olist.source import connect
from olist.verification import code_hashes
from olist.io_utils import hash_file

root=Path('.')
tests={}
for label,name in [('unit','tests.xml'),('integration','integration-tests.xml')]:
    execution=json.loads((root/'evidence'/name.replace('.xml','.receipt.json')).read_text())
    assert execution['exit_code']==0 and execution['code_unchanged']
    assert execution['code_sha256']==code_hashes(), 'Tests must be rerun after code changes'
    assert execution['xml_sha256']==hash_file(root/'evidence'/name)
    tree=ET.parse(root/'evidence'/name)
    cases=tree.findall('.//testcase')
    assert cases and not tree.findall('.//failure') and not tree.findall('.//error') and not tree.findall('.//skipped')
    tests[label]=len(cases)
path,receipt=resolve_publication('data/local')
oracle=json.loads(Path('evidence/final-source-oracle.json').read_text())
dashboard=json.loads(Path('evidence/final-dashboard.json').read_text())
stream=json.loads(Path('evidence/experiment-c.json').read_text())
benchmark=json.loads(Path('evidence/final-local-benchmark.json').read_text())
for check in [oracle,dashboard,stream,benchmark]: assert check['status']=='passed'
for check in [oracle,dashboard,benchmark]:assert check['generation']==receipt['generation']
assert stream['ack_missing_from_raw']==0 and stream['recovered_omitted_run']==2
with connect() as conn:
    jobs=conn.execute("SELECT id,state,exit_code FROM orchestration_jobs WHERE id LIKE 'olist_local_delivery/%' ORDER BY id").fetchall()
successful_runs={}
for job in jobs:
    successful_runs.setdefault(job[0].rsplit('/',1)[0],[]).append(job)
assert any(len(run)==3 and all(j[1:]==('succeeded',0) for j in run) for run in successful_runs.values())
with duckdb.connect(str(path),read_only=True) as conn:
    dictionary=conn.execute("SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='main' ORDER BY table_name,ordinal_position").fetchall()
Path('docs/LOCAL_DICTIONARY.json').write_text(json.dumps([dict(zip(['table','column','type','nullable'],r)) for r in dictionary],indent=2))
source_hashes={}
for directory in ['src','sql','tests','app','dags']:
    for p in sorted(Path(directory).rglob('*')):
        if p.is_file() and '__pycache__' not in p.parts:
            source_hashes[p.as_posix()]=hashlib.sha256(p.read_bytes()).hexdigest()
report={'verified_at':datetime.now(timezone.utc).isoformat(),'status':'passed','tests':tests,
        'generation':receipt['generation'],'dataset_version':receipt['dataset_version'],'summary':receipt['summary'],
        'accounting_totals':{k:sum(t[k] for t in receipt['quality']['tables']) for k in ['raw','accepted','rejected','duplicate']},
        'source_oracle':oracle,'dashboard':dashboard,'airflow_jobs':jobs,
        'stream':{k:stream[k] for k in ['unique_valid_run','duplicate_physical_run','online_unique_run','recovered_omitted_run','ack_missing_from_raw']},
        'benchmark_median_ms':benchmark['median_ms'],'code_sha256':source_hashes,
        'limits':['Local Docker; Spark local[2]; one Kafka broker','Full snapshot analytics; no CDC/incremental claim',
                  'Airflow dags test; no scheduled soak','No fresh Snowflake/dbt/Power BI/cloud deployment',
                  'No hosted GitHub Actions run','Benchmark is warm single-host query measurement; daily mart was slower in this run']}
Path('evidence/FINAL_VERIFICATION.json').write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k!='code_sha256'},indent=2))
