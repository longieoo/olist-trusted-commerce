import io
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from .storage import Store, require
from .contracts import KEYS, digest

def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""): raise ValueError("Unsafe SQL identifier")
    return '"' + value.upper() + '"'

def connection():
    import snowflake.connector
    require(["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_ROLE"])
    return snowflake.connector.connect(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"], warehouse=os.environ["SNOWFLAKE_WAREHOUSE"], database=os.environ["SNOWFLAKE_DATABASE"], schema=os.environ["SNOWFLAKE_SCHEMA"], role=os.environ["SNOWFLAKE_ROLE"], login_timeout=20, network_timeout=60, session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 120, "QUERY_TAG": "olist-bounded-load"})

def load(args):
    if not args.cloud_authorized: raise RuntimeError("Cloud execution requires explicit authorization and --cloud-authorized")
    if bool(args.batch_id) == bool(args.session): raise ValueError("Choose exactly one batch-id or session")
    store = Store(); tables = {}; metrics = None
    if args.batch_id:
        if not re.fullmatch(r"[a-f0-9]{24}", args.batch_id): raise ValueError("Invalid batch ID")
        manifest = store.committed(f"curated/{args.batch_id}/commit.json", f"curated/{args.batch_id}")
        version = manifest["dataset_version"]; kind = manifest["dataset_kind"]; load_id = manifest["batch_id"]
        for f in manifest["files"]:
            if not f["table"].startswith("quarantine_"): tables.setdefault(f["table"], []).append(f)
    else:
        from .replay import safe_name
        session = safe_name(args.session); version = session; kind = "lifecycle_replay"
        for key in store.keys(f"lifecycle/{session}/recovered/"):
            if key.endswith("/commit.json"):
                manifest = store.committed(key, key.rsplit("/",1)[0]); tables.setdefault("lifecycle_events", []).extend(manifest["files"])
        if not tables: raise ValueError("Run finite reconciliation before warehouse load")
        load_id = digest(sorted(f["sha256"] for parts in tables.values() for f in parts))[:24]
        reports = list(Path("evidence").glob(f"reconciliation-{session}-*.json"))
        metrics = [json.loads(p.read_text()) for p in reports]
        load_id = digest([load_id, metrics])[:24]
    with connection() as conn, tempfile.TemporaryDirectory() as temp:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        snowflake_version = cur.fetchone()[0]
        cur.execute("CREATE TABLE IF NOT EXISTS LOAD_AUDIT (LOAD_ID VARCHAR, DATASET_VERSION VARCHAR, LOADED_AT TIMESTAMP_LTZ, ROWS_LOADED NUMBER)")
        if cur.execute("SELECT COUNT(*) FROM LOAD_AUDIT WHERE LOAD_ID=%s AND DATASET_VERSION=%s", (load_id, version)).fetchone()[0]:
            return {"status": "already_committed", "load_id": load_id}
        # DDL occurs before BEGIN; Snowflake DDL implicitly commits transactions.
        stage = identifier("STAGE_" + load_id); cur.execute(f"CREATE TEMPORARY STAGE {stage} FILE_FORMAT=(TYPE=PARQUET USE_LOGICAL_TYPE=TRUE)")
        if metrics:
            import pyarrow as pa, pyarrow.parquet as pq
            path = Path(temp) / "metrics.parquet"
            rows = [{k: v for k,v in r.items() if k not in ["states", "ingestion_lag_seconds"]} | {"mean_ingestion_lag_seconds": r["ingestion_lag_seconds"]["mean"], "max_ingestion_lag_seconds": r["ingestion_lag_seconds"]["max"]} for r in metrics]
            pq.write_table(pa.Table.from_pylist(rows), path)
            key = f"lifecycle/{args.session}/warehouse-metrics/{load_id}.parquet"; store.put(key, path.read_bytes(), immutable=True)
            tables["replay_metrics"] = [store.describe(key, len(rows))]
        loaded = 0; temp_tables = {}
        for table, parts in tables.items():
            target = identifier(table); staging = identifier("TEMP_" + table)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {target} (DATA VARIANT, ROW_KEY VARCHAR, DATASET_VERSION VARCHAR, DATASET_KIND VARCHAR)")
            cur.execute(f"CREATE TEMPORARY TABLE {staging} (DATA VARIANT)"); temp_tables[table] = staging
            # Every manifest part is independently verified and copied into an isolated load directory.
            for i, part in enumerate(parts):
                path = Path(temp) / f"{table}_{i}.parquet"; content = store.get(part["key"])
                import hashlib, pyarrow.parquet as pq
                if hashlib.sha256(content).hexdigest() != part["sha256"]: raise ValueError("Part changed before warehouse load")
                if pq.read_metadata(io.BytesIO(content)).num_rows != part["rows"]: raise ValueError("Part row count mismatch")
                path.write_bytes(content)
                cur.execute(f"PUT 'file://{path.as_posix()}' @{stage}/{table} AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
            cur.execute(f"COPY INTO {staging}(DATA) FROM (SELECT $1 FROM @{stage}/{table}) FILE_FORMAT=(TYPE=PARQUET USE_LOGICAL_TYPE=TRUE) ON_ERROR=ABORT_STATEMENT")
            copied = cur.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
            if copied != sum(f["rows"] for f in parts): raise ValueError("COPY completeness failure")
            loaded += copied
        if args.fail == "before-warehouse-commit": raise RuntimeError("Injected pre-transaction failure")
        try:
            cur.execute("BEGIN")
            for table, staging in temp_tables.items():
                target = identifier(table)
                keys = KEYS.get(table)
                if table == "lifecycle_events": keys = ["replay_run_id", "event_id"]
                if table == "replay_metrics": keys = ["replay_run_id"]
                if keys:
                    expr = "SHA2(TO_JSON(ARRAY_CONSTRUCT(" + ",".join(f'DATA:{k}::VARCHAR' for k in keys) + ")),256)"
                    selection = f"SELECT DATA,{expr} ROW_KEY FROM {staging} QUALIFY ROW_NUMBER() OVER(PARTITION BY {expr} ORDER BY TO_JSON(DATA))=1"
                else:
                    # Geolocation is a multiset: preserve identical occurrences, no ZIP-level uniqueness claim.
                    selection = f"SELECT DATA,SHA2(TO_JSON(DATA)||':'||ROW_NUMBER() OVER(PARTITION BY TO_JSON(DATA) ORDER BY TO_JSON(DATA)),256) ROW_KEY FROM {staging}"
                cur.execute(f"MERGE INTO {target} t USING ({selection}) s ON t.ROW_KEY=s.ROW_KEY AND t.DATASET_VERSION=%s WHEN MATCHED THEN UPDATE SET DATA=s.DATA,DATASET_KIND=%s WHEN NOT MATCHED THEN INSERT(DATA,ROW_KEY,DATASET_VERSION,DATASET_KIND) VALUES(s.DATA,s.ROW_KEY,%s,%s)", (version, kind, version, kind))
            cur.execute("INSERT INTO LOAD_AUDIT SELECT %s,%s,CURRENT_TIMESTAMP(),%s", (load_id, version, loaded))
            if args.fail == "during-warehouse-transaction": raise RuntimeError("Injected transactional failure")
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK"); raise
        if args.fail == "after-warehouse-commit": raise RuntimeError("Injected post-commit failure")
        result = {"status": "committed", "load_id": load_id, "rows_copied": loaded, "tables": list(tables), "snowflake_version": snowflake_version}
        Path(f"evidence/warehouse-{load_id}.json").write_text(json.dumps(result, indent=2)); return result

def run_dbt(directory=None):
    if os.getenv("OLIST_CLOUD_AUTHORIZED") != "1": raise RuntimeError("Set OLIST_CLOUD_AUTHORIZED=1 only after explicit cloud authorization")
    environment = os.environ.copy()
    if directory:
        from .fixtures import verified_manifest
        environment['DBT_DATASET_VERSION'] = verified_manifest(directory)['dataset_version']
    require(["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"])
    if not re.fullmatch(r'[a-f0-9]{64}', environment.get('DBT_DATASET_VERSION','')): raise ValueError('Invalid dataset version')
    result = subprocess.run(["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"], check=True, env=environment)
    return {"exit_code": result.returncode}
