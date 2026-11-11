"""Bounded local query benchmark; warm cache, not end-to-end throughput."""
import json
import platform
import statistics
import time
from pathlib import Path
import duckdb
from olist.local_warehouse import resolve_publication

path,receipt=resolve_publication('data/local')
queries={
 'fact_scan':"SELECT customer_state,count(*) orders,sum(merchandise_brl) merchandise,sum(late_eligible) eligible,sum(coalesce(is_late,0)) late FROM fact_orders WHERE purchase_date BETWEEN DATE '2018-01-01' AND DATE '2018-01-31' GROUP BY customer_state ORDER BY customer_state",
 'daily_mart':"SELECT customer_state,sum(orders) orders,sum(merchandise_brl) merchandise,sum(eligible_delivered) eligible,sum(late_orders) late FROM mart_delivery_daily WHERE purchase_date BETWEEN DATE '2018-01-01' AND DATE '2018-01-31' GROUP BY customer_state ORDER BY customer_state"
}
samples={k:[] for k in queries}
with duckdb.connect(str(path),read_only=True) as conn:
    conn.execute('SET threads=2')
    expected=conn.execute(queries['fact_scan']).fetchall()
    for q in queries.values(): assert conn.execute(q).fetchall()==expected
    for i in range(7):
        for k in (list(queries) if i%2==0 else list(reversed(queries))):
            start=time.perf_counter(); got=conn.execute(queries[k]).fetchall();elapsed=time.perf_counter()-start
            assert got==expected
            samples[k].append(elapsed*1000)
result={'generation':receipt['generation'],'status':'passed','python':platform.python_version(),'duckdb':duckdb.__version__,
        'rows_in_fact':receipt['summary']['orders'],'repetitions':7,'equivalent_results':True,
        'milliseconds':samples,'median_ms':{k:statistics.median(v) for k,v in samples.items()},'queries':queries,
        'limits':'Single host, 2 DuckDB threads, warm caches, alternating variants; other services may be running. Measures query execution+fetch only. Not whole-pipeline throughput or scale-out evidence.'}
Path('evidence/final-local-benchmark.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
