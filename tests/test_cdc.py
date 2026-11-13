from copy import deepcopy
import pytest
from olist.cdc import normalize
from test_incremental import state, source


def envelope():
    return {'source':{'schema':'olist_sim','name':'test','table':'order_payments','lsn':123},'op':'u',
            'after':{'order_id':'o1','payment_sequential':1,'payment_type':'credit_card','payment_installments':1,'payment_value':'0.10'},
            'before':None,'transaction':{'id':'7:123','total_order':2}}


def test_normalization_uses_source_position_not_ingestion_clock():
    payload=envelope(); first=normalize(payload,'test')
    payload['ts_ms']=999999
    assert normalize(payload,'test')==first
    assert first['position']==[123,2] and first['after']['payment_value']=='0.10'
    payload['before']=payload['after'];payload['after']=None;payload['op']='d'
    event=normalize(payload,'test')
    assert event['after'] is None and event['key']=={'order_id':'o1','payment_sequential':1}


@pytest.mark.parametrize('kind',['source','schema','metadata'])
def test_cdc_fails_closed(kind):
    payload=envelope()
    if kind=='source':payload['source']['schema']='public'
    if kind=='schema':payload['after']['new_field']='changed'
    if kind=='metadata':payload['transaction']=None
    with pytest.raises(ValueError):normalize(payload,'test')


def test_cdc_timestamp_precision_never_silently_truncates():
    from olist.contracts import HEADERS
    row={c:None for c in HEADERS['orders']}
    row.update(order_id='o',customer_id='c',order_status='created',
               order_purchase_timestamp=1514764800000)
    payload=envelope()
    payload['source']['table']='orders'
    payload['after']=row
    assert normalize(payload,'test')['after']['order_purchase_timestamp']=='2018-01-01 00:00:00'
    payload['after']['order_purchase_timestamp']+=123
    with pytest.raises(ValueError,match='sub-second'):
        normalize(payload,'test')


def test_empty_snapshot_does_not_create_an_empty_change_batch(tmp_path,monkeypatch):
    import json
    import confluent_kafka
    import olist.cdc as cdc
    class OfflineConsumer:
        def __init__(self,*args,**kwargs): pass
        def subscribe(self,*args): pass
        def close(self): pass
    monkeypatch.setattr(confluent_kafka,'Consumer',OfflineConsumer)
    def reject_empty_batch(*args,**kwargs):
        pytest.fail('An empty snapshot must not be submitted to the change engine')
    monkeypatch.setattr(cdc,'apply',reject_empty_batch)
    monkeypatch.setattr(cdc,'publish_state',lambda root:{'generation':'empty-baseline'})
    (tmp_path/'cdc.json').write_text(json.dumps({'namespace':'test',
        'snapshot_counts':{t:0 for t in cdc.TABLES}}))
    result=cdc.consume(tmp_path,seconds=0)
    assert result['status']=='passed'
    assert result['snapshot_records']==result['pending_transactions']==0
    assert result['batches']==[] and result['generation']=='empty-baseline'


@pytest.mark.parametrize('include_begin',[False,True])
def test_end_without_all_rows_never_claims_caught_up(state,monkeypatch,include_begin):
    import json
    import sqlite3
    from datetime import datetime
    from decimal import Decimal
    import duckdb
    import confluent_kafka
    from olist.cdc import consume
    from olist.incremental import TABLES
    from olist.contracts import HEADERS
    class OfflineConsumer:
        def __init__(self,*args,**kwargs): pass
        def subscribe(self,*args): pass
        def close(self): pass
    monkeypatch.setattr(confluent_kafka,'Consumer',OfflineConsumer)
    rows=[]; counts={}
    with duckdb.connect(str(state/'working.duckdb')) as db:
        for table in TABLES:
            data=db.execute(f'SELECT {",".join(HEADERS[table])} FROM silver_{table}').fetchall()
            counts[table]=len(data)
            for i,record in enumerate(data):
                after=dict(zip(HEADERS[table],record))
                for k,v in after.items():
                    if v is not None and (k.endswith(('_date','_timestamp')) or k=='order_approved_at'):
                        after[k]=int((datetime.fromisoformat(str(v))-datetime(1970,1,1)).total_seconds()*1000)
                    elif isinstance(v,Decimal):after[k]=str(v)
                payload={'source':{'name':'test','schema':'olist_sim','table':table},'op':'r','after':after,'before':None}
                rows.append(('test.olist_sim.'+table,0,i,json.dumps(payload)))
    if include_begin:rows.append(('test.transaction',0,0,json.dumps({'status':'BEGIN','id':'7:100'})))
    rows.append(('test.transaction',0,1,json.dumps({'status':'END','id':'7:110','event_count':1,
        'data_collections':[{'data_collection':'olist_sim.orders','event_count':1}]})))
    (state/'cdc.json').write_text(json.dumps({'namespace':'test','snapshot_counts':counts}))
    with sqlite3.connect(state/'inbox.sqlite') as journal:
        journal.execute('CREATE TABLE records(topic TEXT,partition_id INT,offset_id INT,payload TEXT,PRIMARY KEY(topic,partition_id,offset_id))')
        journal.executemany('INSERT INTO records VALUES (?,?,?,?)',rows)
    result=consume(state,seconds=0)
    assert result['status']=='waiting_for_transactions'
    assert result['pending_transactions']>0 and result['complete_transactions']==0
