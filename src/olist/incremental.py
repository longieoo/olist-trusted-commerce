"""Transactional, single-writer change application with tombstones and audit.

Facts refresh impacted orders; aggregate marts rebuild. Publication copies the
whole database. This is a bounded local lab, not distributed exactly-once.
"""
import json
import os
import shutil
import time
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

from .contracts import HEADERS, KEYS, digest, utc_now
from .io_utils import hash_file
from .local_warehouse import atomic_json, reconcile, resolve_publication
from .models import build, definitions, SQL

TABLES = ('orders', 'order_items', 'order_payments')
CONTRACT = 1


def transform_signature():
    from . import models
    return digest([hash_file(SQL),hash_file(Path(models.__file__)),hash_file(Path(__file__))])


def ensure_transform(conn):
    signature=transform_signature()
    conn.execute('CREATE TABLE IF NOT EXISTS state_transform(signature VARCHAR)')
    previous=conn.execute('SELECT signature FROM state_transform').fetchone()
    changed=not previous or previous[0]!=signature
    if changed:
        build(conn)
        conn.execute('DELETE FROM state_transform')
        conn.execute('INSERT INTO state_transform VALUES (?)',[signature])
    return changed


@contextmanager
def writer(root):
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    lock = root/'.writer.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY); os.close(fd)
    try:
        yield root
    finally:
        lock.unlink(missing_ok=True)


def json_text(value):
    return json.dumps(value, default=str, sort_keys=True, ensure_ascii=False)


def initialize(source_root, root, namespace):
    import duckdb
    if not namespace or len(namespace)>120:
        raise ValueError('Namespace required')
    source, receipt = resolve_publication(source_root)
    with writer(root) as root:
        binding = {'namespace':namespace, 'baseline_generation':receipt['generation'],
                   'baseline_sha256':receipt['database_sha256'], 'contract_version':CONTRACT}
        if (root/'binding.json').exists():
            if json.loads((root/'binding.json').read_text()) != binding:
                raise ValueError('Simulation namespace or baseline changed; use a new state directory')
            if not (root/'working.duckdb').exists():
                raise ValueError('Missing working state')
            return binding
        tmp = root/'initializing.duckdb'; shutil.copyfile(source,tmp)
        with duckdb.connect(str(tmp)) as conn:
            build(conn)
            conn.execute('CREATE TABLE change_versions(table_name VARCHAR,key_json VARCHAR,position VARCHAR,payload_hash VARCHAR,deleted BOOLEAN,PRIMARY KEY(table_name,key_json))')
            conn.execute('CREATE TABLE change_events(event_id VARCHAR PRIMARY KEY,payload_hash VARCHAR,table_name VARCHAR,key_json VARCHAR,position VARCHAR,outcome VARCHAR,before_json VARCHAR,after_json VARCHAR,received_at VARCHAR)')
            conn.execute('CREATE TABLE change_batches(batch_id VARCHAR PRIMARY KEY,receipt VARCHAR)')
            conn.execute('CREATE TABLE simulation_binding AS SELECT ?::VARCHAR binding', [json_text(binding)])
            reconcile(conn)
        os.replace(tmp,root/'working.duckdb')
        atomic_json(root/'baseline.json',receipt)
        atomic_json(root/'binding.json',binding)
        return binding


def validate(event, namespace):
    from .batch import row_reason
    required={'event_id','namespace','contract_version','table','op','key','position','after'}
    if (not isinstance(event,dict) or set(event)!=required
            or type(event['contract_version']) is not int
            or event['contract_version']!=CONTRACT or event['namespace']!=namespace):
        raise ValueError('Change contract or namespace mismatch')
    table=event['table']
    if table not in TABLES or event['op'] not in ('upsert','delete'):
        raise ValueError('Unsupported change table or operation')
    if not isinstance(event['event_id'],str) or not event['event_id']:
        raise ValueError('Missing event identity')
    pos=event['position']
    if not isinstance(pos,list) or len(pos)!=2 or any(type(p)!=int or not 0<=p<10**20 for p in pos):
        raise ValueError('Position must be [source sequence, transaction order]')
    key=event['key']
    if not isinstance(key,dict) or set(key)!=set(KEYS[table]) or any(v in (None,'') for v in key.values()):
        raise ValueError('Invalid natural key')
    # One representation per natural key: no int/string aliases in the version ledger.
    for k,v in key.items():
        if k=='order_id' and (not isinstance(v,str) or not v): raise ValueError('Invalid order key')
        if k!='order_id' and (type(v)!=int or v<1): raise ValueError('Invalid sequence key')
    after=event['after']
    if event['op']=='delete':
        if after is not None: raise ValueError('Delete must have null after')
    else:
        if not isinstance(after,dict) or set(after)!=set(HEADERS[table]) or any(after[k]!=key[k] for k in key):
            raise ValueError('Full after image with matching key required')
        reason=row_reason(table,{k:'' if v is None else str(v) for k,v in after.items()})
        if reason: raise ValueError('Invalid after image: '+reason)
    return table,json_text(key),''.join(f'{p:020d}' for p in pos)


def decimal_oracle(conn):
    """Independent Python Decimal accumulation; no analytical SQL aggregation."""
    totals={r[0]:[Decimal(0),Decimal(0),Decimal(0)] for r in conn.execute('SELECT order_id FROM silver_orders').fetchall()}
    for oid,price,freight in conn.execute('SELECT order_id,price,freight_value FROM silver_order_items').fetchall():
        totals[oid][0]+=Decimal(str(price)); totals[oid][1]+=Decimal(str(freight))
    for oid,payment in conn.execute('SELECT order_id,payment_value FROM silver_order_payments').fetchall():
        totals[oid][2]+=Decimal(str(payment))
    facts={r[0]:list(r[1:]) for r in conn.execute('SELECT order_id,merchandise_brl,freight_brl,payment_brl FROM fact_orders').fetchall()}
    if facts!=totals: raise ValueError('Independent Decimal oracle failed')
    return {'orders_compared':len(totals),'mismatches':0}


def parity(conn):
    """Full rebuild in an isolated database at the same transaction cutoff."""
    import duckdb
    from collections import Counter
    def rows(connection, name):
        # Floating descriptive statistics have a declared 1e-8 comparison precision.
        return Counter(json_text([round(v,8) if isinstance(v,float) else v for v in row])
                       for row in connection.execute(f'SELECT * FROM {name}').fetchall())
    with duckdb.connect() as reference:
        reference.execute("SET threads=2")
        for table in HEADERS:
            data=conn.execute(f'SELECT * FROM silver_{table}').fetch_arrow_table()
            reference.register('_input',data)
            reference.execute(f'CREATE TABLE silver_{table} AS SELECT * FROM _input')
            reference.unregister('_input')
        build(reference)
        result={}
        for name,_ in definitions():
            actual,expected=rows(conn,name),rows(reference,name)
            result[name]=sum((actual-expected).values())+sum((expected-actual).values())
        if any(result.values()): raise ValueError('Full rebuild parity failed: '+json_text(result))
        return result


def apply(root, events, fail=None):
    import duckdb
    if not isinstance(events,list) or not events: raise ValueError('Nonempty transaction batch required')
    with writer(root) as root:
        binding=json.loads((root/'binding.json').read_text())
        signature=transform_signature()
        batch_id=digest([events,signature])
        raw=root/'raw'; raw.mkdir(exist_ok=True)
        raw_path=raw/(batch_id+'.json')
        if raw_path.exists() and json.loads(raw_path.read_text(encoding='utf-8'))!=events:
            raise ValueError('Raw log collision')
        if not raw_path.exists(): atomic_json(raw_path,events)
        started=time.perf_counter()
        with duckdb.connect(str(root/'working.duckdb')) as conn:
            actual=json.loads(conn.execute('SELECT binding FROM simulation_binding').fetchone()[0])
            if actual!=binding: raise ValueError('State binding mismatch')
            old=conn.execute('SELECT receipt FROM change_batches WHERE batch_id=?',[batch_id]).fetchone()
            if old: return json.loads(old[0])
            conn.execute('BEGIN')
            try:
                rebuilt=ensure_transform(conn)
                impacted=set(); counts={'applied':0,'duplicate':0,'stale':0}
                for event in events:
                    table,key_json,position=validate(event,binding['namespace']); fp=digest(event)
                    prior=conn.execute('SELECT payload_hash FROM change_events WHERE event_id=?',[event['event_id']]).fetchone()
                    if prior:
                        if prior[0]!=fp: raise ValueError('Event identity reused with changed payload')
                        counts['duplicate']+=1; continue
                    version=conn.execute('SELECT position,payload_hash FROM change_versions WHERE table_name=? AND key_json=?',[table,key_json]).fetchone()
                    semantic=digest([event['op'],event['key'],event['after']])
                    if version and version[0]==position and version[1]!=semantic:
                        raise ValueError('Conflicting change at the same source position')
                    where=' AND '.join(k+'=?' for k in KEYS[table]); params=[event['key'][k] for k in KEYS[table]]
                    cur=conn.execute(f'SELECT * FROM silver_{table} WHERE {where}',params)
                    row=cur.fetchone(); before=dict(zip([c[0] for c in cur.description],row)) if row else None
                    outcome='stale' if version and position<=version[0] else 'applied'
                    if outcome=='applied':
                        impacted.add(event['key']['order_id'])
                        conn.execute(f'DELETE FROM silver_{table} WHERE {where}',params)
                        if event['op']=='upsert':
                            cols=HEADERS[table]
                            conn.execute(f'INSERT INTO silver_{table} ({",".join(cols)}) VALUES ({",".join("?" for _ in cols)})',[event['after'][k] for k in cols])
                        conn.execute('INSERT OR REPLACE INTO change_versions VALUES (?,?,?,?,?)',
                            [table,key_json,position,semantic,event['op']=='delete'])
                    counts[outcome]+=1
                    conn.execute('INSERT INTO change_events VALUES (?,?,?,?,?,?,?,?,?)',
                        [event['event_id'],fp,table,key_json,position,outcome,json_text(before),json_text(event['after']),utc_now()])
                conn.execute('CREATE OR REPLACE TEMP TABLE impacted_orders(order_id VARCHAR PRIMARY KEY)')
                if impacted:
                    conn.executemany('INSERT INTO impacted_orders VALUES (?)',[(x,) for x in sorted(impacted)])
                    build(conn,incremental=True)
                checks=reconcile(conn); oracle=decimal_oracle(conn); differences=parity(conn)
                receipt={'batch_id':batch_id,'namespace':binding['namespace'],'contract_version':CONTRACT,
                         'counts':counts,'impacted_orders':sorted(impacted),'checks':checks,'decimal_oracle':oracle,
                         'full_rebuild_parity':differences,'finished_at':utc_now(),
                         'seconds':round(time.perf_counter()-started,4),'sql_sha256':hash_file(SQL),
                         'transform_signature':signature,'transform_migration_full_rebuild':rebuilt,
                         'mode':'impacted-order facts; full aggregate refresh; full validation'}
                conn.execute('INSERT INTO change_batches VALUES (?,?)',[batch_id,json_text(receipt)])
                if fail=='before-commit': raise RuntimeError('Injected before commit')
                conn.execute('COMMIT')
            except Exception as exc:
                conn.execute('ROLLBACK')
                quarantine=root/'quarantine'; quarantine.mkdir(exist_ok=True)
                atomic_json(quarantine/(batch_id+'.json'),{'batch_id':batch_id,'error_type':type(exc).__name__,
                    'reason':str(exc),'raw_file':raw_path.name,'received_at':utc_now()})
                raise
        if fail=='after-commit': raise RuntimeError('Injected after commit before acknowledgment')
        return receipt


def publish_state(root, fail=None):
    import duckdb
    with writer(root) as root:
        with duckdb.connect(str(root/'working.duckdb')) as conn:
            conn.execute('BEGIN')
            changed=ensure_transform(conn)
            checks=reconcile(conn); oracle=decimal_oracle(conn)
            if changed: parity(conn)
            conn.execute('COMMIT')
            last=conn.execute('SELECT batch_id FROM change_batches ORDER BY rowid DESC LIMIT 1').fetchone()
            conn.execute('CHECKPOINT')
        baseline=json.loads((root/'baseline.json').read_text())
        binding=json.loads((root/'binding.json').read_text())
        cdc_metadata=root/'cdc.json'
        is_cdc=cdc_metadata.exists()
        if is_cdc and json.loads(cdc_metadata.read_text())['namespace']!=binding['namespace']:
            raise ValueError('CDC publication namespace mismatch')
        kind=('synthetic_postgresql_cdc' if is_cdc else
              'synthetic_corrections_on_olist' if baseline['dataset_kind']=='real_olist' else
              'synthetic_corrections_on_'+baseline['dataset_kind'])
        sha=hash_file(root/'working.duckdb'); generation=sha[:24]
        target=root/(generation+'.duckdb')
        if not target.exists():
            tmp=root/(generation+'.tmp'); shutil.copyfile(root/'working.duckdb',tmp)
            if hash_file(tmp)!=sha: raise ValueError('Publication copy checksum mismatch')
            os.replace(tmp,target)
        elif hash_file(target)!=sha: raise ValueError('Existing publication corrupted')
        receipt={**baseline,'generation':generation,'database':target.name,'database_sha256':sha,
                 'dataset_kind':kind,'published_at':utc_now(),
                 'baseline_dataset_kind':baseline['dataset_kind'],
                 'baseline_source_reference':baseline.get('source_reference'),
                 'source_reference':('Synthetic PostgreSQL olist_sim via Debezium' if is_cdc else
                                     'Synthetic correction events over an immutable baseline'),
                 'checks':checks,'decimal_oracle':oracle,'last_change_batch':last[0] if last else None,
                 'baseline_generation':baseline['generation'],'mode':'incremental simulation; immutable full-file publication',
                 'quality_scope':'Baseline ingestion accounting; changes are fail-closed transactional batches'}
        # Baseline totals would be misleading after a correction.
        receipt.pop('summary',None)
        atomic_json(root/(generation+'.json'),receipt)
        if fail=='before-publication': raise RuntimeError('Injected before publication pointer')
        atomic_json(root/'current.json',receipt)
        return receipt
