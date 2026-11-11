"""Exercise dashboard widgets against independent SQL counts in the publication."""
import json
from datetime import date
from pathlib import Path
import duckdb
from streamlit.testing.v1 import AppTest
from olist.local_warehouse import resolve_publication
from olist.contracts import utc_now
from olist.verification import code_hashes

path,receipt=resolve_publication('data/local')
with duckdb.connect(str(path),read_only=True) as conn:
    expected_all=conn.execute('SELECT count(*) FROM fact_orders').fetchone()[0]
    expected_ba=conn.execute("SELECT count(*) FROM fact_orders WHERE customer_state='BA'").fetchone()[0]
    expected_jan=conn.execute("SELECT count(*) FROM fact_orders WHERE purchase_date BETWEEN DATE '2018-01-01' AND DATE '2018-01-31'").fetchone()[0]
at=AppTest.from_file('app/dashboard.py',default_timeout=60).run()
assert not at.exception,at.exception
assert at.metric[0].value==f'{expected_all:,}'
at.multiselect[0].set_value(['BA']).run()
assert not at.exception and at.metric[0].value==f'{expected_ba:,}'
at.multiselect[0].set_value([]).run()
assert not at.exception and at.metric[0].value==f'{expected_all:,}'
at.date_input[0].set_value((date(2018,1,1),date(2018,1,31))).run()
assert not at.exception and at.metric[0].value==f'{expected_jan:,}'
source_checks={}
for label,root in [('Mô phỏng correction · dữ liệu tách riêng','data/simulation'),('CDC PostgreSQL · nguồn mô phỏng','data/cdc')]:
    if not Path(root+'/current.json').exists(): continue
    db,pub=resolve_publication(root)
    with duckdb.connect(str(db),read_only=True) as conn:
        expected=conn.execute('SELECT count(*) FROM fact_orders').fetchone()[0]
    view=AppTest.from_file('app/dashboard.py',default_timeout=60).run()
    next(widget for widget in view.selectbox if label in widget.options).select(label).run()
    assert not view.exception and view.metric[0].value==f'{expected:,}'
    view.text_input[0].set_value('order-not-present').run()
    assert not view.exception
    source_checks[root]={'generation':pub['generation'],'orders':expected,'missing_order_trace':'passed'}
report={'status':'passed','checked_at':utc_now(),'generation':receipt['generation'],'all_orders':expected_all,'BA_orders':expected_ba,
        'january_2018_orders':expected_jan,'checks':['initial render','state filter','clear filter','date range','no Streamlit exceptions']}
report.update(source_checks=source_checks,code_sha256=code_hashes())
Path('evidence/final-dashboard.json').write_text(json.dumps(report,indent=2))
print(json.dumps({k:v for k,v in report.items() if k!='code_sha256'},indent=2))
