import json
from copy import deepcopy
from decimal import Decimal

import duckdb
import pytest

from test_local_warehouse import source
from olist.incremental import initialize, apply, publish_state
from olist.local_warehouse import publish, resolve_publication
from olist.contracts import HEADERS, KEYS


@pytest.fixture
def state(source,tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    _,manifest,store=source
    publish(manifest['batch_id'],tmp_path/'baseline',store=store)
    initialize(tmp_path/'baseline',tmp_path/'state','test')
    return tmp_path/'state'


def change(root,table,where,event_id='one',position=None,patch=None):
    with duckdb.connect(str(root/'working.duckdb')) as conn:
        cur=conn.execute(f'SELECT {",".join(HEADERS[table])} FROM silver_{table} WHERE {where}')
        row=dict(zip([c[0] for c in cur.description],cur.fetchone()))
    row={k:str(v) if isinstance(v,Decimal) else v for k,v in row.items()}
    row.update(patch or {})
    return {'event_id':event_id,'namespace':'test','contract_version':1,'table':table,
            'op':'upsert','key':{k:row[k] for k in KEYS[table]},'position':position or [1,1],'after':row}


def test_payment_correction_retry_delete_stale_and_oracle(state):
    event=change(state,'order_payments',"order_id='o1' AND payment_sequential=1",patch={'payment_value':'40.00'})
    result=apply(state,[event]); assert result['counts']['applied']==1
    assert result['decimal_oracle']['mismatches']==0 and not any(result['full_rebuild_parity'].values())
    assert apply(state,[event])==result
    delete={**event,'event_id':'delete','position':[2,1],'op':'delete','after':None}
    apply(state,[delete])
    stale={**event,'event_id':'old-retry'}
    assert apply(state,[stale])['counts']['stale']==1
    with duckdb.connect(str(state/'working.duckdb')) as conn:
        assert conn.execute("SELECT payment_count,payment_brl FROM fact_orders WHERE order_id='o1'").fetchone()==(1,Decimal('26.60'))
        assert conn.execute('SELECT count(*) FROM change_versions WHERE deleted').fetchone()[0]==1


def test_atomic_failure_then_ack_loss_and_publication_failure(state):
    event=change(state,'order_payments',"order_id='o1' AND payment_sequential=1",patch={'payment_value':'40.00'})
    first=publish_state(state)
    with pytest.raises(RuntimeError,match='before commit'): apply(state,[event],fail='before-commit')
    with duckdb.connect(str(state/'working.duckdb')) as conn:
        assert conn.execute('SELECT count(*) FROM change_events').fetchone()[0]==0
    with pytest.raises(RuntimeError,match='after commit'): apply(state,[event],fail='after-commit')
    assert apply(state,[event])['counts']['applied']==1
    with pytest.raises(RuntimeError,match='publication'): publish_state(state,fail='before-publication')
    assert resolve_publication(state)[1]['generation']==first['generation']
    assert publish_state(state)['generation']!=first['generation']


def test_conflicts_schema_orphans_and_order_delete_fail_closed(state):
    event=change(state,'order_payments',"order_id='o1' AND payment_sequential=1",patch={'payment_value':'40.00'})
    apply(state,[event])
    changed=deepcopy(event); changed['after']['payment_value']='41.00'
    with pytest.raises(ValueError,match='identity reused'): apply(state,[changed])
    changed['event_id']='different'
    with pytest.raises(ValueError,match='same source position'): apply(state,[changed])
    changed['position']=[2,1]; changed['after']['new_column']='unsupported'
    with pytest.raises(ValueError,match='Full after image'): apply(state,[changed])
    parent=change(state,'orders',"order_id='o1'",event_id='parent-delete',position=[3,1])
    parent.update(op='delete',after=None)
    with pytest.raises(ValueError,match='fk_'): apply(state,[parent])
    parent=change(state,'orders',"order_id='o5'",event_id='empty-delete',position=[3,1])
    parent.update(op='delete',after=None)
    apply(state,[parent])
    with duckdb.connect(str(state/'working.duckdb')) as conn:
        assert conn.execute("SELECT count(*) FROM fact_orders WHERE order_id='o5'").fetchone()[0]==0


def test_old_new_group_keys_and_insert(state):
    event=change(state,'orders',"order_id='o3'",patch={'order_purchase_timestamp':'2017-12-01 00:00:00'})
    # o3 is not delivered; clearing later timestamps makes this correction chronological.
    event['after']['order_approved_at']=None
    event['after']['order_delivered_carrier_date']=None
    event['after']['order_delivered_customer_date']=None
    result=apply(state,[event]); assert result['impacted_orders']==['o3']
    inserted=deepcopy(event); inserted['event_id']='insert'; inserted['key']['order_id']='new'
    inserted['after']['order_id']='new'; inserted['position']=[2,1]
    apply(state,[inserted])
    with duckdb.connect(str(state/'working.duckdb')) as conn:
        assert conn.execute("SELECT count(*) FROM fact_orders WHERE purchase_date=DATE '2017-12-01'").fetchone()[0]==2


def test_missing_is_not_zero_and_cohort_maturity(state):
    with duckdb.connect(str(state/'working.duckdb')) as conn:
        assert conn.execute("SELECT has_payment_records FROM fact_orders WHERE order_id='o5'").fetchone()==(False,)
        assert conn.execute('SELECT count(*) FROM mart_customer_cohorts WHERE repeat_customers>mature_customers OR mature_customers>observed_customers').fetchone()[0]==0
        assert conn.execute('SELECT count(*) FROM fact_order_geography').fetchone()[0]==5
        assert conn.execute('SELECT count(*)-count(DISTINCT zip_prefix) FROM dim_zip').fetchone()[0]==0
        assert conn.execute("SELECT mature_customers,repeat_customers FROM mart_customer_cohorts WHERE cohort_month=DATE '2018-01-01' AND horizon_days=30").fetchone()==(1,1)
        assert conn.execute('SELECT sum(mature_customers) FROM mart_customer_cohorts WHERE horizon_days=90').fetchone()[0]==0
        lat,lon,observations=conn.execute("SELECT latitude,longitude,observations FROM dim_zip WHERE zip_prefix='00123'").fetchone()
        assert lat==pytest.approx(-23.555) and lon==pytest.approx(-46.635) and observations==2
