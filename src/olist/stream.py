import io
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from .contracts import validate_event, RANK
from .storage import Store
from .batch import spark_session
from .replay import safe_name

def raw_schema():
    from pyspark.sql import types as T
    return T.StructType([T.StructField("payload", T.StringType()), T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType()), T.StructField("kafka_timestamp", T.TimestampType()), T.StructField("ingested_at", T.TimestampType())])

def parse_frame(raw):
    from pyspark.sql import functions as F, types as T
    event_schema = T.StructType([T.StructField(k, t) for k, t in [("event_id", T.StringType()), ("schema_version", T.IntegerType()), ("dataset_version", T.StringType()), ("replay_run_id", T.StringType()), ("order_id", T.StringType()), ("event_type", T.StringType()), ("source_event_time", T.StringType()), ("event_time", T.StringType()), ("emitted_at", T.StringType()), ("is_simulated", T.BooleanType())]])
    def reason(payload):
        try: return validate_event(json.loads(payload))
        except (ValueError, TypeError): return "malformed_json"
    check = F.udf(reason, T.StringType())
    return (raw.withColumn("reason", check("payload")).withColumn("event", F.from_json("payload", event_schema))
        .select("*", "event.*").drop("event")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("emitted_at", F.to_timestamp("emitted_at"))
        .withColumn("ingestion_lag_seconds", F.col("ingested_at").cast("double") - F.col("emitted_at").cast("double")))

def publish_frame(frame, store, prefix, batch_id):
    commit = f"{prefix}/batches/{batch_id}/commit.json"
    old = store.maybe(commit)
    if old:
        store.verify(json.loads(old), f"{prefix}/batches/{batch_id}"); return
    attempt = uuid.uuid4().hex; out = f"{prefix}/batches/{batch_id}/attempts/{attempt}"
    frame.repartition(2).write.mode("errorifexists").parquet(f"s3a://{store.bucket}/{out}")
    import pyarrow.parquet as pq
    files = []
    for key in store.keys(out + "/"):
        if key.endswith(".parquet"):
            n = pq.read_metadata(io.BytesIO(store.get(key))).num_rows
            files.append(store.describe(key, n))
    manifest = {"schema_version": 1, "microbatch_id": batch_id, "files": files}
    store.verify(manifest, f"{prefix}/batches/{batch_id}"); store.json(commit, manifest, immutable=True)

def consume(args):
    from pyspark.sql import functions as F
    session = safe_name(args.session); store = Store()
    binding = {'rule_version':1,'topic':os.getenv('KAFKA_TOPIC','olist-lifecycle-v1'),
        'bootstrap':os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),'watermark':'10 seconds'}
    binding_key = f'lifecycle/{session}/binding.json'
    local_binding = Path(f'/checkpoints/{session}/binding.json')
    old_binding = store.maybe(binding_key)
    if old_binding and (not local_binding.exists() or json.loads(local_binding.read_text()) != binding):
        raise ValueError('Session checkpoint binding missing or changed; use a new session')
    if not old_binding and (store.keys(f'lifecycle/{session}/raw/') or local_binding.parent.exists()):
        raise ValueError('Legacy session has no binding; use a new session')
    store.json(binding_key,binding,immutable=True)
    local_binding.parent.mkdir(parents=True,exist_ok=True)
    from .local_warehouse import atomic_json
    atomic_json(local_binding,binding)
    spark = spark_session("olist-stream-" + session)
    prefix = f"lifecycle/{session}"; raw_path = f"s3a://{store.bucket}/{prefix}/raw"
    checkpoint = f"/checkpoints/{session}"; queries = []
    def start_raw():
        kafka = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
                 .option("subscribe", os.getenv("KAFKA_TOPIC", "olist-lifecycle-v1")).option("startingOffsets", "earliest").option("failOnDataLoss", "true").load())
        raw = kafka.select(F.col("value").cast("string").alias("payload"), "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"), F.current_timestamp().alias("ingested_at"))
        writer = raw.writeStream.format("parquet").option("path", raw_path).option("checkpointLocation", checkpoint + "/raw")
        return (writer.trigger(processingTime="2 seconds") if args.continuous else writer.trigger(availableNow=True)).start()
    try:
        qraw = start_raw(); queries.append(qraw)
        if not args.continuous: qraw.awaitTermination()
        else:
            while not store.keys(prefix + "/raw/_spark_metadata/"):
                if qraw.exception(): raise qraw.exception()
                if not qraw.isActive: raise RuntimeError("Raw query stopped before publication")
                time.sleep(1)
        raw = spark.readStream.schema(raw_schema()).parquet(raw_path)
        valid = parse_frame(raw).filter("reason IS NULL").drop("reason")
        # Raw persistence precedes this irreversible, bounded online deduplication.
        online = valid.withWatermark("event_time", "10 seconds").dropDuplicatesWithinWatermark(["replay_run_id", "event_id"])
        def sink(frame, batch_id):
            publish_frame(frame, store, prefix + "/online", batch_id)
            if os.getenv("OLIST_FAIL_STREAM_AFTER_COMMIT") == "1": raise RuntimeError("Injected consumer interruption after sink commit")
        writer = online.writeStream.foreachBatch(sink).option("checkpointLocation", checkpoint + "/online")
        qonline = (writer.trigger(processingTime="2 seconds") if args.continuous else writer.trigger(availableNow=True)).start(); queries.append(qonline)
        if args.continuous:
            while True:
                for query in queries:
                    if query.exception(): raise query.exception()
                time.sleep(1)
        else: qonline.awaitTermination()
        result = {"session": session, "raw_progress": list(qraw.recentProgress), "online_progress": list(qonline.recentProgress)}
        Path("evidence").mkdir(exist_ok=True); Path(f"evidence/stream-{session}-{time.time_ns()}.json").write_text(json.dumps(result, indent=2, default=str))
        return result
    finally:
        for query in queries:
            if query.isActive: query.stop()
        spark.stop()

def committed_online(spark, store, session):
    paths = []
    for key in store.keys(f"lifecycle/{session}/online/batches/"):
        if key.endswith("/commit.json"):
            manifest = store.committed(key, key.rsplit("/", 1)[0])
            paths.extend(f"s3a://{store.bucket}/{f['key']}" for f in manifest["files"])
    return spark.read.parquet(*paths) if paths else None

def reconcile(session, run):
    from pyspark.sql import functions as F, Window
    session = safe_name(session); safe_name(run); store = Store(); spark = spark_session("olist-reconcile")
    try:
        raw = spark.read.parquet(f"s3a://{store.bucket}/lifecycle/{session}/raw").persist()
        parsed = parse_frame(raw).persist(); valid = parsed.filter("reason IS NULL").filter(F.col("replay_run_id") == run)
        # First observation wins; raw offsets retain all physical duplicates.
        unique = valid.withColumn("rn", F.row_number().over(Window.partitionBy("replay_run_id", "event_id").orderBy("topic", "partition", "offset"))).filter("rn=1").drop("rn", "reason")
        online = committed_online(spark, store, session)
        online = online.filter(F.col("replay_run_id") == run) if online is not None else unique.limit(0)
        rank = F.create_map(*[x for k, v in RANK.items() for x in (F.lit(k), F.lit(v))])
        state = unique.withColumn("milestone_rank", rank[F.col("event_type")]).withColumn("rn", F.row_number().over(Window.partitionBy("replay_run_id", "order_id").orderBy(F.desc("milestone_rank"), F.desc("event_time"), F.desc("event_id")))).filter("rn=1").drop("rn")
        quarantine = parsed.filter("reason IS NOT NULL")
        # Recovery is a finite snapshot, never merged into historical order facts.
        from .contracts import digest
        offsets = [(r.topic, r.partition, r.offset) for r in raw.select("topic", "partition", "offset").collect()]
        generation = digest(sorted(offsets))[:24]
        for name, frame in [("recovered", unique), ("state", state), ("quarantine", quarantine)]:
            publish_frame(frame, store, f"lifecycle/{session}/{name}/{run}", generation)
        unique_count = unique.count(); online_count = online.select("event_id").distinct().count()
        lag = valid.agg(F.avg("ingestion_lag_seconds").alias("mean"), F.max("ingestion_lag_seconds").alias("max")).first().asDict()
        journals = list(Path("data/runtime/replay").glob(run + "*.sqlite")); ack_offsets = set()
        for journal in journals:
            with sqlite3.connect(journal) as db:
                ack_offsets.update(db.execute("SELECT ack_topic,ack_partition,ack_offset FROM outbox WHERE ack_offset IS NOT NULL").fetchall())
        observed = set(offsets)
        result = {"session": session, "replay_run_id": run, "recovery_generation": generation, "raw_all_runs": raw.count(), "valid_physical_all_runs": parsed.filter("reason IS NULL").count(), "valid_physical_run": valid.count(), "quarantined_all_runs": quarantine.count(), "unique_valid_run": unique_count, "duplicate_physical_run": valid.count()-unique_count, "online_physical_run": online.count(), "online_unique_run": online_count, "recovered_omitted_run": unique.select("event_id").join(online.select("event_id"), "event_id", "left_anti").count(), "acknowledged_physical": len(ack_offsets), "ack_missing_from_raw": len(ack_offsets-observed), "state_rows": state.count(), "ingestion_lag_seconds": lag, "states": [r.asDict() for r in state.select("order_id", "event_type", "milestone_rank").collect()]}
        from .contracts import utc_now
        result['reconciled_at'] = utc_now()
        assert result['raw_all_runs'] == result['valid_physical_all_runs'] + result['quarantined_all_runs']
        Path(f"evidence/reconciliation-{session}-{run}.json").write_text(json.dumps(result, indent=2))
        return result
    finally: spark.stop()
