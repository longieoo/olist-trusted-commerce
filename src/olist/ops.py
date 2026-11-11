import csv
import json
import os
import platform
import subprocess
from decimal import Decimal
from pathlib import Path

def doctor():
    from .storage import Store
    from .source import connect
    import importlib.metadata as metadata
    result = {"python": platform.python_version(), "platform": platform.platform(), "cpu_count": os.cpu_count(), "packages": {p: metadata.version(p) for p in ["boto3", "psycopg", "pyarrow", "dbt-snowflake", "snowflake-connector-python", "confluent-kafka"]}, "java": subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()[0], "spark": subprocess.run(["/opt/spark/bin/spark-submit", "--version"], capture_output=True, text=True).stderr, "credentials_present": {k: bool(os.getenv(k)) for k in ["POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]}}
    with connect() as conn: result["postgres_version"] = conn.execute("SELECT version()").fetchone()[0]
    result["minio_bucket"] = Store().bucket
    Path("evidence/environment.json").write_text(json.dumps(result, indent=2))
    return result

def latest_fixture():
    from .storage import Store, batch_id
    manifest = json.loads(Path("data/fixtures/manifest.json").read_text()); bid = batch_id(manifest["dataset_version"])
    store = Store(); return store, store.committed(f"curated/{bid}/commit.json", f"curated/{bid}")

def smoke():
    from .fixtures import write_fixture
    from .source import seed, extract
    from .batch import process, spark_session, read_table
    source = write_fixture("data/fixtures"); seed("data/fixtures")
    raw = extract(source["dataset_version"]); result = process(raw)
    assert process(extract(source["dataset_version"])) == result
    store, committed = latest_fixture(); spark = spark_session("olist-financial-reconciliation")
    try:
        for table in ["orders", "order_items", "order_payments", "customers", "order_reviews", "geolocation"]:
            read_table(spark, store, committed, table).createOrReplaceTempView(table)
        totals = spark.sql("""WITH i AS (SELECT order_id,SUM(price) merchandise,SUM(freight_value) freight,COUNT(*) item_count FROM order_items GROUP BY order_id),
        p AS (SELECT order_id,SUM(payment_value) payment,COUNT(*) payment_count FROM order_payments GROUP BY order_id)
        SELECT i.*,p.payment,p.payment_count FROM i JOIN p USING(order_id) WHERE order_id='o1'""").first().asDict()
        assert totals["merchandise"] == Decimal("60.60") and totals["freight"] == Decimal("6.00") and totals["payment"] == Decimal("66.60")
        assert totals["item_count"] == 3 and totals["payment_count"] == 2
        count = spark.sql("SELECT COUNT(*) n FROM orders").first().n; assert count == 5
        assert spark.sql("SELECT COUNT(*) n FROM geolocation").first().n == 2
        assert spark.sql("SELECT COUNT(*) n FROM order_reviews WHERE order_id='o1'").first().n == 2
        parts = [f for f in committed["files"] if f["table"] == "order_items"]
        assert len(parts) >= 2 and sum(f["rows"] for f in parts) == 4
        report = {"status": "passed", "batch_id": committed["batch_id"], "orders": count, "o1": {k: str(v) if isinstance(v, Decimal) else v for k,v in totals.items()}, "accounting": committed["accounting"], "item_parts_read": len(parts), "stable_rerun": True}
        Path("evidence/smoke.json").write_text(json.dumps(report, indent=2)); return report
    finally: spark.stop()

def export_fixture():
    from .batch import spark_session, read_table
    from pyspark.sql import functions as F, Window
    store, manifest = latest_fixture(); spark = spark_session("olist-fixture-export")
    root = Path("data/exports/fixtures"); root.mkdir(parents=True, exist_ok=True)
    try:
        frames = {t: read_table(spark, store, manifest, t) for t in ["orders", "customers", "order_items", "order_payments", "order_reviews", "sellers", "products"]}
        orders = frames["orders"].join(frames["customers"], "customer_id")
        for name, start, end in [("purchase_to_carrier_days", "order_purchase_timestamp", "order_delivered_carrier_date"), ("carrier_to_customer_days", "order_delivered_carrier_date", "order_delivered_customer_date"), ("purchase_to_delivery_days", "order_purchase_timestamp", "order_delivered_customer_date")]:
            orders = orders.withColumn(name, (F.to_timestamp(end).cast("long") - F.to_timestamp(start).cast("long")) / 86400.0)
        orders = orders.withColumn("purchase_date", F.to_date("order_purchase_timestamp")).withColumn("late_eligible", ((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull() & F.col("order_estimated_delivery_date").isNotNull()).cast("int"))
        orders = orders.withColumn("is_late", F.when(F.col("late_eligible") == 1, (F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date")).cast("int")))
        reviews = frames["order_reviews"].withColumn("rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc_nulls_last("review_answer_timestamp"), F.desc_nulls_last("review_creation_date"), F.desc("review_id")))).filter("rn=1").select("order_id", "review_score")
        orders = orders.join(reviews, "order_id", "left")
        exports = {"fact_orders": orders, "fact_order_items": frames["order_items"], "fact_payments": frames["order_payments"], "dim_seller": frames["sellers"], "dim_product": frames["products"], "bridge_order_seller": frames["order_items"].select("order_id", "seller_id").distinct()}
        replay_parts=[]
        for key in store.keys('lifecycle/demo/recovered/demo/batches/'):
            if key.endswith('/commit.json'):
                committed=store.committed(key,key.rsplit('/',1)[0])
                replay_parts.extend(f"s3a://{store.bucket}/{part['key']}" for part in committed['files'])
        if replay_parts:
            from .contracts import RANK
            events=spark.read.parquet(*replay_parts).dropDuplicates(['replay_run_id','event_id'])
            rank=F.create_map(*[x for k,v in RANK.items() for x in (F.lit(k),F.lit(v))])
            events=events.withColumn('milestone_rank',rank[F.col('event_type')])
            exports['fact_lifecycle_events']=events.drop('payload')
            exports['replay_order_state']=events.withColumn('rn',F.row_number().over(Window.partitionBy('replay_run_id','order_id').orderBy(F.desc('milestone_rank'),F.desc('event_time'),F.desc('event_id')))).filter('rn=1').drop('rn','payload')
        for name, frame in exports.items():
            with (root / f"{name}.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=frame.columns); writer.writeheader()
                for row in frame.toLocalIterator(): writer.writerow(row.asDict())
        metric_path=Path('evidence/reconciliation-demo-demo.json')
        if metric_path.exists():
            metric=json.loads(metric_path.read_text())
            metric_row={k:v for k,v in metric.items() if k not in ['states','ingestion_lag_seconds']}
            metric_row.update({'mean_ingestion_lag_seconds':metric['ingestion_lag_seconds']['mean'],'max_ingestion_lag_seconds':metric['ingestion_lag_seconds']['max']})
            with (root/'replay_run_metrics.csv').open('w',encoding='utf-8',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(metric_row)); writer.writeheader(); writer.writerow(metric_row)
        result = {"dataset_kind": "test_fixtures", "batch_id": manifest["batch_id"], "files": sorted(p.name for p in root.glob("*.csv")), "warning": "Synthetic fixture exports; not evidence of Snowflake/dbt execution."}
        (root / "provenance.json").write_text(json.dumps(result, indent=2)); return result
    finally: spark.stop()
