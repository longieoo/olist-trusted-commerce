import json
import os
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from .contracts import HEADERS, KEYS, MONEY, INTEGERS, STATUSES, money
from .storage import Store

def spark_session(name):
    from pyspark.sql import SparkSession
    jars = ",".join(str(p) for p in Path("/opt/spark/extra-jars").glob("*.jar"))
    builder = (SparkSession.builder.master("local[2]").appName(name)
        .config("spark.driver.memory", "1g").config("spark.sql.shuffle.partitions", os.getenv('OLIST_SHUFFLE_PARTITIONS', '2'))
        .config("spark.sql.session.timeZone", "UTC").config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    events = os.getenv('OLIST_SPARK_EVENT_DIR')
    if events:
        Path(events).mkdir(parents=True, exist_ok=True)
        builder = builder.config('spark.eventLog.enabled', 'true').config('spark.eventLog.dir', Path(events).resolve().as_uri())
    spark = builder.getOrCreate(); spark.sparkContext.setLogLevel("ERROR")
    return spark

def row_reason(table, row):
    try:
        for key in KEYS.get(table, ["geolocation_zip_code_prefix"]):
            if not row.get(key): return "missing_key"
        for key, value in row.items():
            if value in [None, ""]: continue
            if key in MONEY: money(value)
            if key in INTEGERS:
                number = Decimal(value)
                if not number.is_finite() or number != number.to_integral_value() or not 0 <= number <= 2147483647: return "invalid_integer"
            if key.endswith("_date") or key.endswith("_timestamp") or key in ["order_approved_at", "review_answer_timestamp"]:
                datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        if table == "orders":
            if row["order_status"] not in STATUSES: return "invalid_status"
            if not row["order_purchase_timestamp"] or not row["customer_id"]: return "missing_required"
            stamps = [row[k] for k in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"] if row[k]]
            if stamps != sorted(stamps): return "invalid_chronology"
        if table == "order_items":
            if any(not row[k] for k in ["product_id", "seller_id", "price", "freight_value"]): return "missing_required"
            if int(Decimal(row["order_item_id"])) < 1: return "invalid_sequence"
        if table == "order_payments":
            if not row["payment_value"] or int(Decimal(row["payment_sequential"])) < 1: return "invalid_payment"
        if table == "order_reviews" and (not row["review_score"] or not 1 <= int(Decimal(row["review_score"])) <= 5): return "invalid_review_score"
        if table == "customers" and not row["customer_unique_id"]: return "missing_customer_unique_id"
        if table == "geolocation":
            if not -90 <= float(row["geolocation_lat"]) <= 90 or not -180 <= float(row["geolocation_lng"]) <= 180: return "invalid_coordinate"
        for key, value in row.items():
            if key.endswith("zip_code_prefix") and (not 1 <= len(value) <= 5 or not value.isdigit()): return "invalid_zip"
    except (ValueError, TypeError, OverflowError, InvalidOperation): return "invalid_type"
    return None

def process(raw, fail=None):
    from pyspark.sql import functions as F, types as T, Window
    store = Store(); bid = raw["batch_id"]; commit = f"curated/{bid}/commit.json"
    old = store.maybe(commit)
    if old: return store.verify(json.loads(old), f"curated/{bid}")
    store.verify(raw, f"raw/{bid}")
    attempt = uuid.uuid4().hex; prefix = f"curated/{bid}/attempts/{attempt}"
    spark = spark_session("olist-batch"); accepted = {}; files = []; counts = {}; schemas = {}
    foreign = {"orders": [("customer_id", "customers", "customer_id")], "order_items": [("order_id", "orders", "order_id"), ("product_id", "products", "product_id"), ("seller_id", "sellers", "seller_id")], "order_payments": [("order_id", "orders", "order_id")], "order_reviews": [("order_id", "orders", "order_id")]}
    order = ["customers", "sellers", "products", "category_translation", "geolocation", "orders", "order_items", "order_payments", "order_reviews"]
    try:
        for table in order:
            paths = [f"s3a://{store.bucket}/{e['key']}" for e in raw["files"] if e["table"] == table]
            if not paths: raise ValueError("Missing declared table: " + table)
            schema = T.StructType([T.StructField(c, T.StringType(), True) for c in HEADERS[table]])
            # source.extract uses csv.DictWriter: embedded quotes are doubled (RFC 4180).
            df = spark.read.schema(schema).option("header", True).option("multiLine", True).option("escape", '"').option("mode", "FAILFAST").csv(paths)
            raw_count = df.count()
            declared = sum(e["rows"] for e in raw["files"] if e["table"] == table)
            if declared != raw_count: raise ValueError("CSV reconciliation failed")
            # Preserve raw nulls in source; validation uses normalized empty values only here.
            check = F.udf(lambda row, t=table: row_reason(t, {k: (v or "") for k, v in row.asDict().items()}), T.StringType())
            df = df.withColumn("_reason", check(F.struct(*HEADERS[table]))).withColumn("_source_hash", F.sha2(F.to_json(F.struct(*HEADERS[table])), 256))
            for col, ref_table, ref_col in foreign.get(table, []):
                ref = accepted[ref_table].select(F.col(ref_col).alias("_fk")).distinct()
                df = df.join(ref, df[col] == ref["_fk"], "left").withColumn("_reason", F.when(F.col("_reason").isNull() & F.col("_fk").isNull(), F.lit("orphan_" + col)).otherwise(F.col("_reason"))).drop("_fk")
            df = df.persist(); rejected = df.filter(F.col("_reason").isNotNull())
            good = df.filter(F.col("_reason").isNull())
            if table in KEYS:
                good = good.withColumn("_rn", F.row_number().over(Window.partitionBy(*KEYS[table]).orderBy("_source_hash")))
                duplicates = good.filter("_rn > 1").drop("_rn").withColumn("_reason", F.lit("duplicate_key"))
                good = good.filter("_rn = 1").drop("_rn")
            else: duplicates = df.limit(0)
            count_reject = rejected.count(); count_dup = duplicates.count(); good = good.drop("_reason", "_source_hash")
            for c in HEADERS[table]:
                if c in MONEY: good = good.withColumn(c, F.col(c).cast(T.DecimalType(18, 2)))
                elif c in INTEGERS: good = good.withColumn(c, F.col(c).cast("decimal(18,0)").cast("int"))
                elif c.endswith("zip_code_prefix"): good = good.withColumn(c, F.lpad(F.col(c), 5, "0"))
                elif c.startswith("geolocation_") and c in ["geolocation_lat", "geolocation_lng"]: good = good.withColumn(c, F.col(c).cast("double"))
                else: good = good.withColumn(c, F.when(F.col(c) == "", F.lit(None).cast("string")).otherwise(F.col(c)))
            accepted[table] = good.persist(); count_good = good.count()
            counts[table] = {"raw": raw_count, "accepted": count_good, "rejected": count_reject, "duplicate": count_dup}
            if raw_count != count_good + count_reject + count_dup: raise AssertionError("Accounting invariant failed")
            schemas[table] = good.schema.jsonValue()
            for kind, frame in [(table, good), (f"quarantine_{table}", rejected.unionByName(duplicates))]:
                out = f"{prefix}/{kind}"; frame.repartition(2).write.mode("errorifexists").parquet(f"s3a://{store.bucket}/{out}")
                for key in store.keys(out + "/"):
                    if key.endswith(".parquet"):
                        import pyarrow.parquet as pq
                        import io
                        n = pq.read_metadata(io.BytesIO(store.get(key))).num_rows
                        entry = store.describe(key, n); entry["table"] = kind; files.append(entry)
            df.unpersist()
        result = {"schema_version": 1, "batch_id": bid, "attempt_id": attempt, "dataset_version": raw["dataset_version"], "dataset_kind": raw["dataset_kind"], "interval": raw["interval"], "files": files, "accounting": counts, "schemas": schemas}
        store.verify(result, f"curated/{bid}")
        if fail == "before-curated-commit": raise RuntimeError("Injected failure before curated commit")
        store.json(commit, result, immutable=True)
        if fail == "after-curated-commit": raise RuntimeError("Injected failure after curated commit")
        Path("evidence").mkdir(exist_ok=True)
        Path(f"evidence/batch-{bid}.json").write_text(json.dumps(result, indent=2))
        return result
    finally: spark.stop()

def read_table(spark, store, manifest, table):
    paths = [f"s3a://{store.bucket}/{f['key']}" for f in manifest["files"] if f["table"] == table]
    if not paths: raise ValueError("Missing committed parts: " + table)
    df = spark.read.parquet(*paths)
    if df.count() != sum(f["rows"] for f in manifest["files"] if f["table"] == table): raise ValueError("Parquet count mismatch")
    return df
