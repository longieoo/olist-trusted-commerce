import json
import os
import statistics
import time
import uuid
from pathlib import Path
from .batch import spark_session, read_table
from .ops import latest_fixture
from .storage import Store

def benchmark():
    from pyspark.sql import functions as F
    spark = spark_session("olist-controlled-experiments"); store, manifest = latest_fixture()
    root = "data/runtime/benchmark/" + uuid.uuid4().hex; Path(root).mkdir(parents=True)
    report = {"dataset_kind": "benchmark_expansion", "source_dataset_version": manifest["dataset_version"], "spark_version": spark.version, "hadoop_version": spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion(), "master": spark.sparkContext.master, "cpu_limit": 2, "memory_limit_gib": 3, "automatic_broadcast": -1, "aqe": False, "repetitions": 3, "warmups": 1, "cache_policy": "No Spark cache; warm OS/object-store caches not flushed; order rotated by repetition", "storage": [], "joins": []}
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1); spark.conf.set("spark.sql.adaptive.enabled", "false")
        orders = read_table(spark, store, manifest, "orders")
        customers = read_table(spark, store, manifest, "customers")
        items = read_table(spark, store, manifest, "order_items")
        payments = read_table(spark, store, manifest, "order_payments")
        replicas = spark.range(5000).select(F.col("id").alias("replica"))
        def expand(df, keys):
            df = df.crossJoin(replicas)
            for key in keys: df = df.withColumn(key, F.concat_ws("_", F.col(key), F.col("replica")))
            return df.drop("replica")
        eorders = expand(orders, ["order_id", "customer_id"])
        ecustomers = expand(customers, ["customer_id", "customer_unique_id"])
        eitems = expand(items, ["order_id"]); epayments = expand(payments, ["order_id"])
        # Referenced products and sellers remain unchanged shared dimensions.
        assert eitems.select("order_id").join(eorders.select("order_id"), "order_id", "left_anti").count() == 0
        assert epayments.select("order_id").join(eorders.select("order_id"), "order_id", "left_anti").count() == 0
        assert eorders.select("customer_id").join(ecustomers.select("customer_id"), "customer_id", "left_anti").count() == 0
        amounts = eitems.groupBy("order_id").agg(F.sum("price").alias("merchandise_brl"))
        data = eorders.join(ecustomers.select("customer_id", "customer_state"), "customer_id").join(amounts, "order_id", "left").select("order_id", "customer_state", F.to_date("order_purchase_timestamp").alias("purchase_date"), F.coalesce("merchandise_brl", F.lit(0)).cast("decimal(18,2)").alias("merchandise_brl"))
        data.write.mode("errorifexists").option("header",True).csv(root+"/csv")
        data.write.mode("errorifexists").parquet(root+"/parquet")
        data.write.mode("errorifexists").partitionBy("purchase_date").parquet(root+"/partitioned")
        report["rows"] = data.count(); report["partitions"] = 2; report["replicas"] = 5000
        variants = {
            "csv": lambda: spark.read.schema(data.schema).option("header",True).csv(root+"/csv"),
            "parquet": lambda: spark.read.parquet(root+"/parquet"),
            "partitioned_parquet": lambda: spark.read.parquet(root+"/partitioned"),
        }
        def query(frame):
            return frame.filter("purchase_date='2018-01-01'").groupBy("customer_state").agg(F.count("*").alias("orders"),F.sum("merchandise_brl").alias("merchandise")).orderBy("customer_state")
        expected = [r.asDict() for r in query(data).collect()]
        for name, reader in variants.items(): query(reader()).collect()
        timings = {name: [] for name in variants}
        names = list(variants)
        for rep in range(3):
            for name in names[rep:]+names[:rep]:
                spark.catalog.clearCache(); start=time.perf_counter(); got=[r.asDict() for r in query(variants[name]()).collect()]; elapsed=time.perf_counter()-start
                assert got == expected; timings[name].append(elapsed)
        for name, values in timings.items():
            path=root+('/partitioned' if name=='partitioned_parquet' else '/'+name)
            plan=query(variants[name]())._jdf.queryExecution().executedPlan().toString()
            report["storage"].append({"variant":name,"seconds":values,"median":statistics.median(values),"min":min(values),"max":max(values),"bytes":sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file()),"plan":plan})
        small = customers.select("customer_state").distinct().withColumn("label",F.concat(F.lit("state_"),F.col("customer_state")))
        base = spark.read.parquet(root+"/parquet")
        def joined(strategy):
            dim=F.broadcast(small) if strategy=='broadcast' else small.hint('merge')
            return base.hint('merge').join(dim,'customer_state').groupBy('label').agg(F.count('*').alias('orders'),F.sum('merchandise_brl').alias('merchandise')).orderBy('label')
        expected_join=[r.asDict() for r in joined('regular').collect()]
        join_times={n:[] for n in ['regular','broadcast']}
        for name in join_times: joined(name).collect()
        for rep in range(3):
            for name in (['regular','broadcast'] if rep%2==0 else ['broadcast','regular']):
                spark.catalog.clearCache(); start=time.perf_counter(); got=[r.asDict() for r in joined(name).collect()]; elapsed=time.perf_counter()-start
                assert got==expected_join; join_times[name].append(elapsed)
        for name,values in join_times.items():
            plan=joined(name)._jdf.queryExecution().executedPlan().toString()
            assert ('BroadcastHashJoin' in plan) if name=='broadcast' else ('SortMergeJoin' in plan)
            report['joins'].append({'variant':name,'seconds':values,'median':statistics.median(values),'min':min(values),'max':max(values),'plan':plan})
        report['equivalent_results']=True; report['storage_result']=expected; report['join_result']=expected_join
        Path('evidence/experiments-ab.json').write_text(json.dumps(report,indent=2,default=str)); return report
    finally: spark.stop()

def recovery_experiment():
    """Separate bounded Spark invocations exercise persisted watermark/checkpoint state."""
    import subprocess
    import sys
    from datetime import datetime, timedelta, timezone
    from .contracts import digest
    from .replay import send_payloads
    from .stream import reconcile
    run='recovery_'+uuid.uuid4().hex[:10]; session=run
    origin=datetime.now(timezone.utc); version='correctness-fixture-v1'
    def event(order,kind,seconds):
        source='2018-01-01 10:00:00'
        return {'event_id':digest([version,run,order,kind,source]),'schema_version':1,'dataset_version':version,'replay_run_id':run,'order_id':order,'event_type':kind,'source_event_time':source,'event_time':(origin+timedelta(seconds=seconds)).isoformat(),'is_simulated':True}
    def consumer(label,fail=False):
        env=os.environ.copy(); env['OLIST_DEBUG']='1'
        if fail: env['OLIST_FAIL_STREAM_AFTER_COMMIT']='1'
        path=Path(f'evidence/{run}-{label}.log')
        with path.open('w') as f:
            result=subprocess.run([sys.executable,'-m','olist.cli','stream','--session',session],stdout=f,stderr=subprocess.STDOUT,env=env,timeout=180)
        assert (result.returncode!=0) if fail else (result.returncode==0)
        return result.returncode
    started=time.perf_counter()
    # One known order progresses to delivered; a delayed earlier milestone must not regress it.
    first=[event('a','order_created',0),event('a','order_delivered',5)]
    send_payloads(first+[first[0],'{bad'],run+'_phase1',100)
    failure=consumer('interrupted',True); consumer('restart')
    # Advance watermark beyond old events. AvailableNow creates a final no-data batch.
    send_payloads([event('advance','order_created',100)],run+'_phase2',100); consumer('advance')
    send_payloads([event('within','order_created',95),event('a','order_approved',2),event('beyond','order_created',1)],run+'_phase3',100); consumer('late')
    result=reconcile(session,run)
    assert result['ack_missing_from_raw']==0
    assert result['unique_valid_run']==6 and result['duplicate_physical_run']==1
    assert result['online_unique_run']==4 and result['recovered_omitted_run']==2
    assert next(s for s in result['states'] if s['order_id']=='a')['event_type']=='order_delivered'
    elapsed=time.perf_counter()-started
    result.update({'status':'passed','consumer_interruption_exit_code':failure,'elapsed_seconds':elapsed,'acknowledged_per_wall_second_including_startups':result['acknowledged_physical']/elapsed,'watermark_delay_seconds':10,'inside_horizon_event_seconds':95,'max_event_time_seconds_before_late_batch':100,'beyond_horizon_events_seconds':[1,2], 'note':'Throughput includes Spark process startup and recovery; not steady-state capacity.'})
    Path('evidence/experiment-c.json').write_text(json.dumps(result,indent=2)); return result
