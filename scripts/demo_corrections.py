"""Reproducible correction + delete + failure drill on a real Olist baseline."""
import json
import time
from decimal import Decimal
from pathlib import Path
import duckdb
from olist.contracts import HEADERS,KEYS,digest,utc_now
from olist.incremental import initialize,apply,publish_state
from olist.local_warehouse import resolve_publication,atomic_json
from olist.verification import code_hashes

root=Path('data/simulation'); namespace='portfolio-demo'
initialize('data/local',root,namespace)
baseline,_=resolve_publication('data/local')
with duckdb.connect(str(baseline),read_only=True) as db:
    oid=db.execute('SELECT order_id FROM fact_orders WHERE item_count>1 AND payment_count>1 AND late_eligible=1 ORDER BY order_id LIMIT 1').fetchone()[0]
    before=db.execute('SELECT payment_brl,is_late FROM fact_orders WHERE order_id=?',[oid]).fetchone()
    def event(table,where,position,patch=None,delete=False):
        cur=db.execute(f'SELECT {",".join(HEADERS[table])} FROM silver_{table} WHERE {where}',[oid])
        row=dict(zip([c[0] for c in cur.description],cur.fetchone()))
        row={k:str(v) if isinstance(v,Decimal) else v for k,v in row.items()}
        row.update(patch or {})
        return {'event_id':digest([namespace,table,position]),'namespace':namespace,'contract_version':1,
                'table':table,'key':{k:row[k] for k in KEYS[table]},'op':'delete' if delete else 'upsert',
                'position':position,'after':None if delete else row}
    payment=event('order_payments','order_id=? ORDER BY payment_sequential LIMIT 1',[1,1])
    payment['after']['payment_value']=str(Decimal(payment['after']['payment_value'])+Decimal('7.25'))
    delivery=event('orders','order_id=?',[1,2])
    delivery['after']['order_estimated_delivery_date']=delivery['after']['order_delivered_customer_date']
    deleted=event('order_items','order_id=? ORDER BY order_item_id DESC LIMIT 1',[2,1],delete=True)
started=time.perf_counter(); original=publish_state(root)
receipt=apply(root,[payment,delivery])
assert apply(root,[payment,delivery])==receipt
delete_receipt=apply(root,[deleted])
stale={**payment,'event_id':digest(['stale',payment['event_id']]),'position':[0,1]}
stale_receipt=apply(root,[stale])
# After an engine-signature migration, replaying the same stale event is a
# duplicate of an already-recorded stale decision; both outcomes are safe.
assert stale_receipt['counts']['stale']+stale_receipt['counts']['duplicate']==1
prior=(root/'current.json').read_bytes()
try: publish_state(root,fail='before-publication')
except RuntimeError: pass
else: raise AssertionError('Failure injection did not fire')
assert (root/'current.json').read_bytes()==prior
published=publish_state(root)
path,_=resolve_publication(root)
with duckdb.connect(str(path),read_only=True) as db:
    after=db.execute('SELECT payment_brl,is_late FROM fact_orders WHERE order_id=?',[oid]).fetchone()
assert after[0]-before[0]==Decimal('7.25') and after[1]==0
report={'status':'passed','checked_at':utc_now(),'order_id':oid,'baseline_generation':published['baseline_generation'],
        'generation':published['generation'],'before':list(map(str,before)),'after':list(map(str,after)),
        'payment_delta_brl':'7.25','correction':receipt,'delete':delete_receipt,'stale':stale_receipt,
        'publication_failure_kept_previous_pointer':True,'invocation_seconds':round(time.perf_counter()-started,3),
        'timing_scope':'Includes publication copies; existing batches reuse verified receipts. Batch seconds describe their original validation, not this replay or steady-state latency.',
        'code_sha256':code_hashes()}
atomic_json(Path('evidence/upgrade-corrections.json'),report)
print(json.dumps({k:v for k,v in report.items() if k not in ['code_sha256','correction','delete','stale']},indent=2))
