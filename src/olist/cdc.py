"""Debezium PostgreSQL adapter and durable Kafka inbox for the bounded CDC lab.

Only complete transactions are applied. Kafka offsets acknowledge durable inbox
capture, not analytical publication. The inbox is replayed on every invocation.
"""
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

from .contracts import HEADERS, KEYS, MONEY, INTEGERS, digest, utc_now
from .incremental import TABLES, initialize, apply, publish_state, writer
from .local_warehouse import resolve_publication, atomic_json


def request(path, body=None, method=None):
    data=json.dumps(body).encode() if body is not None else None
    with urlopen(Request('http://connect:8083'+path,data=data,method=method,
                         headers={'Content-Type':'application/json'}),timeout=30) as response:
        return json.load(response)


def pg_connect():
    import psycopg
    return psycopg.connect(host='postgres',dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])


def setup(root='data/cdc', source_root='data/local-fixture'):
    import duckdb
    root=Path(root)
    # A stable namespace pins one connector, replication slot and historical baseline.
    namespace='olist_portfolio_cdc'
    initialize(source_root,root,namespace)
    source,receipt=resolve_publication(source_root)
    binding={'namespace':namespace,'baseline_generation':receipt['generation']}
    secret_path=root/'connector-secret.json'
    if not secret_path.exists():
        import secrets
        atomic_json(secret_path,{'password':secrets.token_urlsafe(32)})
        secret_path.chmod(0o600)
    connector_password=json.loads(secret_path.read_text())['password']
    with pg_connect() as pg, duckdb.connect(str(source),read_only=True) as db:
        pg.execute('CREATE SCHEMA IF NOT EXISTS olist_sim')
        pg.execute('CREATE TABLE IF NOT EXISTS olist_sim.binding (singleton INT PRIMARY KEY CHECK(singleton=1),value JSONB NOT NULL)')
        existing=pg.execute('SELECT value FROM olist_sim.binding WHERE singleton=1').fetchone()
        if existing and existing[0]!=binding: raise ValueError('CDC source baseline changed; explicit reset/migration required')
        if not existing:
            for table in TABLES:
                cols=[]
                for col in HEADERS[table]:
                    typ='NUMERIC(18,2)' if col in MONEY else 'INTEGER' if col in INTEGERS else 'TIMESTAMP' if col.endswith(('_date','_timestamp')) or col=='order_approved_at' else 'TEXT'
                    cols.append(col+' '+typ)
                cols.append('PRIMARY KEY ('+','.join(KEYS[table])+')')
                if table!='orders': cols.append('FOREIGN KEY(order_id) REFERENCES olist_sim.orders(order_id)')
                pg.execute(f'CREATE TABLE olist_sim.{table} ({",".join(cols)})')
                data=db.execute(f'SELECT {",".join(HEADERS[table])} FROM silver_{table}').fetchall()
                with pg.cursor().copy(f'COPY olist_sim.{table} FROM STDIN') as copy:
                    for row in data: copy.write_row(row)
                pg.execute(f'ALTER TABLE olist_sim.{table} REPLICA IDENTITY FULL')
            pg.execute('INSERT INTO olist_sim.binding VALUES (1,%s)',[json.dumps(binding)])
        counts={t:pg.execute(f'SELECT count(*) FROM olist_sim.{t}').fetchone()[0] for t in TABLES}
        from psycopg import sql
        if not pg.execute("SELECT 1 FROM pg_roles WHERE rolname='olist_cdc_reader'").fetchone():
            pg.execute(sql.SQL('CREATE ROLE olist_cdc_reader LOGIN REPLICATION PASSWORD {}').format(sql.Literal(connector_password)))
        else:
            pg.execute(sql.SQL('ALTER ROLE olist_cdc_reader LOGIN REPLICATION PASSWORD {}').format(sql.Literal(connector_password)))
        pg.execute('GRANT USAGE ON SCHEMA olist_sim TO olist_cdc_reader')
        for table in TABLES: pg.execute(f'GRANT SELECT ON olist_sim.{table} TO olist_cdc_reader')
        if not pg.execute("SELECT 1 FROM pg_publication WHERE pubname='olist_portfolio_publication'").fetchone():
            pg.execute('CREATE PUBLICATION olist_portfolio_publication FOR TABLE '+','.join('olist_sim.'+t for t in TABLES))
    config={'connector.class':'io.debezium.connector.postgresql.PostgresConnector','tasks.max':'1',
        'database.hostname':'postgres','database.port':'5432','database.user':'olist_cdc_reader',
        'database.password':connector_password,'database.dbname':os.environ['POSTGRES_DB'],
        'topic.prefix':namespace,'plugin.name':'pgoutput','slot.name':'olist_portfolio_slot',
        'publication.name':'olist_portfolio_publication','publication.autocreate.mode':'disabled',
        'table.include.list':','.join('olist_sim.'+t for t in TABLES),'snapshot.mode':'initial',
        'provide.transaction.metadata':'true','decimal.handling.mode':'string','time.precision.mode':'connect',
        'key.converter':'org.apache.kafka.connect.json.JsonConverter','value.converter':'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable':'false','value.converter.schemas.enable':'false',
        'topic.creation.default.replication.factor':'1','topic.creation.default.partitions':'1'}
    if namespace not in request('/connectors'):
        request('/connectors',{'name':namespace,'config':config})
    else:
        request('/connectors/'+namespace+'/config',config,method='PUT')
    metadata=root/'cdc.json'
    if not metadata.exists(): atomic_json(metadata,{'namespace':namespace,'snapshot_counts':counts,'created_at':utc_now()})
    return {'namespace':namespace,'tables':counts,'credentials':'not included in evidence'}


def normalize(payload, namespace):
    source=payload['source']; table=source['table']; op=payload['op']
    if source.get('schema')!='olist_sim' or source.get('name')!=namespace or table not in TABLES or op not in ('r','c','u','d'):
        raise ValueError('Unexpected CDC source or operation')
    row=payload['before'] if op=='d' else payload['after']
    if row is None or set(row)!=set(HEADERS[table]): raise ValueError('CDC schema drift or missing full row')
    row=dict(row)
    for k,v in row.items():
        if v is not None and (k.endswith(('_date','_timestamp')) or k=='order_approved_at'):
            if type(v)!=int: raise ValueError('Expected Connect millisecond timestamp')
            # The shared source contract retains timestamps at whole-second precision.
            # Reject unsupported precision rather than silently changing source values.
            if v % 1000:
                raise ValueError('CDC sub-second timestamps are unsupported by the whole-second source contract')
            row[k]=(datetime(1970,1,1)+timedelta(milliseconds=v)).strftime('%Y-%m-%d %H:%M:%S')
    key={k:row[k] for k in KEYS[table]}
    tx=payload.get('transaction')
    if op!='r' and (not tx or 'total_order' not in tx): raise ValueError('Transaction metadata required')
    position=[0,0] if op=='r' else [int(source['lsn']),int(tx['total_order'])]
    identity=[namespace,table,key,op,position,None if op=='r' else tx['id']]
    return {'event_id':digest(identity),'namespace':namespace,'contract_version':1,'table':table,
            'key':key,'op':'delete' if op=='d' else 'upsert','position':position,'after':None if op=='d' else row}


def consume(root='data/cdc', seconds=20):
    from confluent_kafka import Consumer
    root=Path(root); cfg=json.loads((root/'cdc.json').read_text()); namespace=cfg['namespace']
    # The capture lock prevents multiple consumers from sharing one SQLite journal.
    with writer(root/'capture'):
        with sqlite3.connect(root/'inbox.sqlite') as journal:
            journal.execute('PRAGMA journal_mode=WAL'); journal.execute('PRAGMA synchronous=FULL')
            journal.execute('CREATE TABLE IF NOT EXISTS records(topic TEXT,partition_id INT,offset_id INT,payload TEXT,PRIMARY KEY(topic,partition_id,offset_id))')
            consumer=Consumer({'bootstrap.servers':'kafka:9092','group.id':namespace+'-durable-inbox',
                'auto.offset.reset':'earliest','enable.auto.commit':False,'enable.auto.offset.store':False})
            topics=[namespace+'.olist_sim.'+t for t in TABLES]+[namespace+'.transaction']
            captured=0
            try:
                consumer.subscribe(topics); deadline=time.monotonic()+seconds
                while time.monotonic()<deadline:
                    msg=consumer.poll(1)
                    if msg is None: continue
                    if msg.error():
                        if 'UNKNOWN_TOPIC_OR_PART' in str(msg.error()): continue
                        raise RuntimeError(str(msg.error()))
                    payload=msg.value().decode('utf-8') if msg.value() is not None else 'null'
                    journal.execute('INSERT OR IGNORE INTO records VALUES (?,?,?,?)',
                        [msg.topic(),msg.partition(),msg.offset(),payload]); journal.commit()
                    consumer.commit(message=msg,asynchronous=False); captured+=1
            finally: consumer.close()
            records=journal.execute('SELECT topic,offset_id,payload FROM records ORDER BY topic,partition_id,offset_id').fetchall()
            if len(records)>50000: raise ValueError('CDC lab journal limit exceeded; partition/archive before scaling')
        snapshots=[]; transactions={}; boundaries=[]
        for topic,offset,payload_text in records:
            payload=json.loads(payload_text)
            if payload is None: continue  # Kafka compaction tombstone; delete envelope carries mutation.
            if topic==namespace+'.transaction':
                boundaries.append((offset,payload))
            elif payload.get('op')=='r': snapshots.append(normalize(payload,namespace))
            else:
                tx=payload.get('transaction')
                if not tx: raise ValueError('CDC event missing transaction metadata')
                event=normalize(payload,namespace)
                xid=tx['id'].split(':',1)[0]
                bucket=transactions.setdefault(xid,{})
                if event['event_id'] in bucket and bucket[event['event_id']]!=event: raise ValueError('Conflicting replayed CDC event')
                bucket[event['event_id']]=event
        unique={}
        for event in snapshots:
            if event['event_id'] in unique and unique[event['event_id']]!=event:
                raise ValueError('Conflicting CDC snapshot replay')
            unique[event['event_id']]=event
        actual={t:sum(e['table']==t for e in unique.values()) for t in TABLES}
        if actual!=cfg['snapshot_counts']:
            return {'status':'waiting_for_complete_snapshot','captured':captured,'expected':cfg['snapshot_counts'],'actual':actual}
        # A valid empty baseline has no snapshot mutation batch to apply.
        reports=([apply(root,sorted(unique.values(),key=lambda e:(TABLES.index(e['table']),e['event_id'])))]
                 if unique else [])
        complete=[]; applied_ids=set(); begins={}; seen_boundaries=set(); blocked=False
        for _,boundary in sorted(boundaries):
            boundary_key=(boundary['status'],boundary['id'])
            if boundary_key in seen_boundaries: continue
            seen_boundaries.add(boundary_key)
            xid,lsn=boundary['id'].split(':',1); lsn=int(lsn)
            if boundary['status']=='BEGIN':
                begins[xid]=lsn; continue
            if xid not in begins:
                blocked=True; break
            # PostgreSQL metadata IDs are txID:LSN OF EACH OPERATION, not a
            # constant string across BEGIN/rows/END. Scope xid by its LSN interval.
            events=[e for e in transactions.get(xid,{}).values() if begins[xid]<=e['position'][0]<=lsn]
            expected=int(boundary['event_count'])
            if len(events)!=expected or {e['position'][1] for e in events}!=set(range(1,expected+1)):
                blocked=True; break
            counts={'olist_sim.'+t:sum(e['table']==t for e in events) for t in TABLES}
            if any(counts.get(c['data_collection'])!=c['event_count'] for c in boundary['data_collections']):
                raise ValueError('Transaction collection count mismatch')
            reports.append(apply(root,sorted(events,key=lambda e:e['position'][1])))
            complete.append(boundary['id']); applied_ids.update(e['event_id'] for e in events)
            del begins[xid]
        receipt=publish_state(root)
        pending=max(int(blocked),len(begins),sum(any(eid not in applied_ids for eid in bucket) for bucket in transactions.values()))
        result={'status':'waiting_for_transactions' if pending else 'passed','captured':captured,'inbox_records':len(records),'complete_transactions':len(set(complete)),
                'snapshot_records':len(unique),'generation':receipt['generation'],'batches':[r['batch_id'] for r in reports],
                'pending_transactions':pending,'checked_at':utc_now()}
        atomic_json(root/'last-consume.json',result)
        return result
