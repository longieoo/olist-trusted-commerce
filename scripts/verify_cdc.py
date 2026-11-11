"""Run inside the runtime container after cdc-setup. No secrets in evidence."""
import json
from decimal import Decimal
from pathlib import Path
import duckdb
from olist.cdc import consume,pg_connect,request
from olist.contracts import HEADERS,utc_now
from olist.incremental import TABLES
from olist.local_warehouse import resolve_publication,atomic_json
from olist.verification import code_hashes

def settle(expected_payment,expected_late):
    for _ in range(3):
        result=consume(seconds=15)
        if result['status']=='passed':
            path,_=resolve_publication('data/cdc')
            with duckdb.connect(str(path),read_only=True) as db:
                actual=db.execute("SELECT payment_brl,is_late FROM fact_orders WHERE order_id='o1'").fetchone()
            if actual==(Decimal(expected_payment),expected_late): return result
    raise AssertionError(result)

# Reset only the explicitly synthetic demo order, making every run exercise WAL changes.
with pg_connect() as pg:
    pg.execute("UPDATE olist_sim.order_payments SET payment_value=40.00 WHERE order_id='o1' AND payment_sequential=1")
    pg.execute("INSERT INTO olist_sim.order_payments VALUES ('o1',2,'voucher',1,26.60) ON CONFLICT (order_id,payment_sequential) DO UPDATE SET payment_value=26.60")
    pg.execute("DELETE FROM olist_sim.order_payments WHERE order_id='o1' AND payment_sequential=3")
    pg.execute("UPDATE olist_sim.orders SET order_estimated_delivery_date='2018-01-05 00:00:00' WHERE order_id='o1'")
first=settle('66.60',1)
assert first['status']=='passed',first
before_path,before_receipt=resolve_publication('data/cdc')
with duckdb.connect(str(before_path),read_only=True) as db:
    before=db.execute("SELECT payment_brl,is_late FROM fact_orders WHERE order_id='o1'").fetchone()
# Consumer is closed here. Source commits continue while it is offline.
with pg_connect() as pg:
    pg.execute("UPDATE olist_sim.order_payments SET payment_value=41.25 WHERE order_id='o1' AND payment_sequential=1")
    pg.execute("UPDATE olist_sim.orders SET order_estimated_delivery_date=order_delivered_customer_date WHERE order_id='o1'")
    pg.commit()
    pg.execute("DELETE FROM olist_sim.order_payments WHERE order_id='o1' AND payment_sequential=2")
    pg.execute("INSERT INTO olist_sim.order_payments VALUES ('o1',3,'voucher',1,5.00)")
after=settle('46.25',0)
assert after['status']=='passed' and after['pending_transactions']==0,after
assert after['complete_transactions']-first['complete_transactions']==2
path,receipt=resolve_publication('data/cdc')
with duckdb.connect(str(path),read_only=True) as db, pg_connect() as pg:
    role=pg.execute("SELECT rolsuper,rolreplication FROM pg_roles WHERE rolname='olist_cdc_reader'").fetchone()
    assert role==(False,True)
    assert pg.execute("SELECT has_table_privilege('olist_cdc_reader','olist_sim.orders','SELECT'),has_table_privilege('olist_cdc_reader','olist_sim.orders','UPDATE')").fetchone()==(True,False)
    compared={}
    for table in TABLES:
        cols=','.join(HEADERS[table])
        source=pg.execute(f'SELECT {cols} FROM olist_sim.{table} ORDER BY {",".join(HEADERS[table][:2])}').fetchall()
        actual=db.execute(f'SELECT {cols} FROM silver_{table} ORDER BY {",".join(HEADERS[table][:2])}').fetchall()
        def normalized(rows): return [[None if v is None else str(v) for v in r] for r in rows]
        assert normalized(source)==normalized(actual),table
        compared[table]=len(actual)
    result=db.execute("SELECT payment_brl,is_late FROM fact_orders WHERE order_id='o1'").fetchone()
    assert result==(Decimal('46.25'),0),result
    audit=db.execute('SELECT outcome,count(*) FROM change_events GROUP BY outcome').fetchall()
replayed=consume(seconds=3)
assert replayed['batches']==after['batches']
status=request('/connectors/olist_portfolio_cdc/status')
assert status['connector']['state']=='RUNNING' and all(t['state']=='RUNNING' for t in status['tasks'])
report={'status':'passed','checked_at':utc_now(),'generation':receipt['generation'],
        'source_parity_rows':compared,'payment_expected_brl':'46.25','payment_actual_brl':str(result[0]),
        'late_actual':result[1],'snapshot_records':after['snapshot_records'],
        'before':list(map(str,before)),'new_complete_transactions':after['complete_transactions']-first['complete_transactions'],
        'complete_transactions':after['complete_transactions'],'pending_transactions':after['pending_transactions'],
        'consumer_offline_during_mutations':True,'durable_inbox_replay_same_batches':True,
        'audit':audit,'connector':'Debezium PostgreSQL 3.2.4.Final','connector_state':'RUNNING',
        'connector_role':'replication + SELECT; no superuser or table UPDATE',
        'scope':'Real WAL CDC from deterministic synthetic PostgreSQL tables; single broker, bounded lab',
        'code_sha256':code_hashes()}
atomic_json(Path('evidence/upgrade-cdc.json'),report)
print(json.dumps({k:v for k,v in report.items() if k!='code_sha256'},indent=2))
