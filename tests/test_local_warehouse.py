import csv
import hashlib
import io
import json
from copy import deepcopy
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from olist.contracts import HEADERS, FILES, MONEY, INTEGERS
from olist.fixtures import write_fixture
from olist.local_warehouse import materialize, publish, reconcile, resolve_publication


class MemoryStore:
    bucket='test'
    def __init__(self, manifest, payloads):
        self.manifest=manifest; self.payloads=payloads; self.client=self
    def committed(self, key, prefix):
        return deepcopy(self.manifest)
    def get_object(self, Bucket, Key):
        return {'Body':io.BytesIO(self.payloads[Key])}


@pytest.fixture
def source(tmp_path):
    write_fixture(tmp_path/'csv')
    bid='a'*24
    manifest={'schema_version':1,'batch_id':bid,'dataset_version':'fixture','dataset_kind':'test_fixtures',
              'interval':[None,None],'accounting':{},'files':[]}
    limits={'orders':5,'order_items':4,'order_reviews':3}
    paths={}; payloads={}
    for table,columns in HEADERS.items():
        with (tmp_path/'csv'/FILES[table]).open(encoding='utf-8',newline='') as f:
            rows=list(csv.DictReader(f))
        rows=rows[:limits.get(table,len(rows))]
        for row in rows:
            for k,v in row.items():
                row[k]=None if not v else Decimal(v) if k in MONEY else int(v) if k in INTEGERS else v
        schema=pa.schema([(k,pa.decimal128(18,2) if k in MONEY else pa.int32() if k in INTEGERS else pa.string()) for k in columns])
        good=pa.Table.from_pylist(rows,schema=schema)
        bad=pa.Table.from_pylist([],schema=pa.schema(list(schema)+[pa.field('_reason',pa.string())]))
        for name,data in [(table,good),('quarantine_'+table,bad)]:
            path=tmp_path/f'{name}.parquet'; pq.write_table(data,path)
            key=f'curated/{bid}/attempts/test/{name}.parquet'; payload=path.read_bytes()
            manifest['files'].append({'key':key,'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload),'rows':len(data),'table':name})
            paths[name]=[str(path)]; payloads[key]=payload
        manifest['accounting'][table]={'raw':len(rows),'accepted':len(rows),'rejected':0,'duplicate':0}
    return paths,manifest,MemoryStore(manifest,payloads)


def test_grain_money_and_calendar_day_semantics(source):
    paths,manifest,_=source
    with duckdb.connect() as conn:
        materialize(conn,paths,manifest)
        assert conn.execute("SELECT merchandise_brl,freight_brl,payment_brl,item_count,payment_count FROM fact_orders WHERE order_id='o1'").fetchone()==(Decimal('60.60'),Decimal('6.00'),Decimal('66.60'),3,2)
        assert conn.execute("SELECT order_id,is_late FROM fact_orders ORDER BY order_id").fetchall()==[('o1',1),('o2',0),('o3',None),('o4',None),('o5',None)]
        assert conn.execute("SELECT review_score,review_history_count FROM order_review_summary WHERE order_id='o1'").fetchone()==(3,2)
        # Deliberately offset two orders: aggregate sum stays unchanged.
        conn.execute("UPDATE fact_orders SET merchandise_brl=merchandise_brl+CASE WHEN order_id='o1' THEN 1 WHEN order_id='o2' THEN -1 ELSE 0 END")
        with pytest.raises(ValueError,match='per_order_money'):
            reconcile(conn)


def test_publication_failure_replay_and_tamper(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    _,manifest,store=source; root=tmp_path/'publication'
    receipt=publish(manifest['batch_id'],root,store=store)
    assert receipt==publish(manifest['batch_id'],root,store=store)
    before=(root/'current.json').read_bytes()
    store.manifest['interval']=['2018-01-01','2019-01-01']
    with pytest.raises(RuntimeError,match='Injected'):
        publish(manifest['batch_id'],root,fail='before-publication',store=store)
    assert (root/'current.json').read_bytes()==before
    path,_=resolve_publication(root)
    path.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='checksum'):
        resolve_publication(root)


def test_reject_download_corruption_missing_parts_and_counts(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths,manifest,store=source
    key=manifest['files'][0]['key']; store.payloads[key]=b'corrupt'
    with pytest.raises(ValueError,match='checksum'):
        publish(manifest['batch_id'],tmp_path/'bad',store=store)
    assert not (tmp_path/'bad/current.json').exists()
    with duckdb.connect() as conn:
        altered=deepcopy(manifest); altered['accounting']['orders']['accepted']+=1
        with pytest.raises(ValueError,match='accounting'):
            materialize(conn,paths,altered)


def test_writer_lock_and_bad_quality_never_publish(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    _,manifest,store=source; root=tmp_path/'locked';root.mkdir()
    (root/'.writer.lock').write_text('other writer')
    with pytest.raises(FileExistsError): publish(manifest['batch_id'],root,store=store)
    assert (root/'.writer.lock').exists()
    store.manifest['accounting']['orders']['raw']+=1
    with pytest.raises(ValueError,match='Quality gate'):
        publish(manifest['batch_id'],tmp_path/'quality',store=store)
    assert not (tmp_path/'quality/current.json').exists()


def test_reader_uses_captured_generation(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    _,manifest,store=source; root=tmp_path/'publication'
    old=publish(manifest['batch_id'],root,store=store)
    captured=(root/'current.json').read_bytes()
    store.manifest['interval']=['2018-01-01','2019-01-01']
    new=publish(manifest['batch_id'],root,store=store)
    assert old['generation']!=new['generation']
    assert resolve_publication(root,pointer_bytes=captured)[1]==old
    assert resolve_publication(root)[1]==new


def test_null_keys_and_orphans_block_publication(source):
    paths,manifest,_=source
    with duckdb.connect() as conn:
        materialize(conn,paths,manifest)
        conn.execute("UPDATE silver_order_items SET seller_id='absent' WHERE order_id='o1'")
        with pytest.raises(ValueError,match='fk_order_items_seller_id'): reconcile(conn)
        conn.execute('UPDATE silver_customers SET customer_id=NULL')
        with pytest.raises(ValueError,match='null_key_customers'): reconcile(conn)


def test_part_row_count_is_verified(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    _,manifest,store=source
    store.manifest['files'][0]['rows']+=1
    with pytest.raises(ValueError,match='Part row accounting'):
        publish(manifest['batch_id'],tmp_path/'bad',store=store)
