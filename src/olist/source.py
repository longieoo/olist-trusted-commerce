import csv
import io
import json
import os
import uuid
from pathlib import Path
import psycopg
from .contracts import HEADERS, FILES
from .storage import Store, batch_id, require

def connect():
    require(["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"])
    return psycopg.connect(host=os.getenv("PGHOST", "postgres"), port=os.getenv("PGPORT", "5432"), user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"], dbname=os.environ["POSTGRES_DB"])

DDL = """
CREATE TABLE IF NOT EXISTS source_rows (
 dataset_version text NOT NULL, table_name text NOT NULL, ordinal bigint NOT NULL,
 payload jsonb NOT NULL, PRIMARY KEY(dataset_version,table_name,ordinal));
CREATE TABLE IF NOT EXISTS datasets (dataset_version text PRIMARY KEY, manifest jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS batch_progress (batch_id text NOT NULL, attempt_id text NOT NULL,
 phase text NOT NULL, updated_at timestamptz DEFAULT now(), PRIMARY KEY(batch_id,attempt_id));
"""

def seed(directory):
    from .fixtures import verified_manifest
    root = Path(directory); manifest = verified_manifest(root)
    store = Store(); version = manifest["dataset_version"]; prefix = f"sources/{manifest['dataset_kind']}/{version}"
    # Preserve original bytes, including empty cells, multiline fields and duplicates.
    with connect() as conn:
        conn.execute(DDL)
        conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', (version,))
        conn.execute("CREATE TEMPORARY TABLE seed_rows (LIKE source_rows INCLUDING DEFAULTS) ON COMMIT DROP")
        for entry in manifest["files"]:
            table = entry["table"]; path = root / entry["filename"]
            from .io_utils import hash_file
            if hash_file(path) != entry["sha256"]: raise ValueError("Source changed after inventory")
            store.put_file(f"{prefix}/{entry['filename']}", path, immutable=True)
            with (root / entry["filename"]).open(encoding="utf-8-sig", newline="") as f:
                with conn.cursor().copy("COPY seed_rows (dataset_version,table_name,ordinal,payload) FROM STDIN") as copy:
                    for ordinal, row in enumerate(csv.DictReader(f), 1):
                        copy.write_row((version, table, ordinal, json.dumps(row)))
            conflict = conn.execute('''SELECT count(*) FROM source_rows s JOIN seed_rows t
                USING(dataset_version,table_name,ordinal) WHERE s.payload IS DISTINCT FROM t.payload''').fetchone()[0]
            if conflict: raise ValueError('Immutable source row conflict: ' + table)
            conn.execute("INSERT INTO source_rows SELECT * FROM seed_rows ON CONFLICT DO NOTHING")
            count = conn.execute('SELECT count(*) FROM source_rows WHERE dataset_version=%s AND table_name=%s', (version,table)).fetchone()[0]
            if count != entry['records']: raise ValueError('Source row accounting mismatch: ' + table)
            conn.execute("TRUNCATE seed_rows")
        conn.execute("INSERT INTO datasets VALUES(%s,%s) ON CONFLICT DO NOTHING", (version, json.dumps(manifest)))
    store.json(f"{prefix}/manifest.json", manifest, immutable=True)
    return manifest

def extract(version, start=None, end=None, fail=None):
    from datetime import datetime
    for value in [start, end]:
        if value: datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if start and end and start >= end: raise ValueError("start must be < end")
    store = Store(); bid = batch_id(version, start, end); commit = f"raw/{bid}/commit.json"
    existing = store.maybe(commit)
    if existing: return store.verify(json.loads(existing), f"raw/{bid}")
    attempt = uuid.uuid4().hex; prefix = f"raw/{bid}/attempts/{attempt}"; files = []
    with connect() as conn:
        conn.execute(DDL)
        source = conn.execute("SELECT manifest FROM datasets WHERE dataset_version=%s", (version,)).fetchone()
        if not source: raise ValueError("Unknown dataset version")
        conn.execute("INSERT INTO batch_progress(batch_id,attempt_id,phase) VALUES(%s,%s,'extracting')", (bid, attempt))
        conn.commit()
        # Repeatable-read protects the bounded snapshot; seed is immutable/versioned.
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        import tempfile
        from .io_utils import csv_parts
        max_rows = int(os.getenv('OLIST_EXTRACT_PART_ROWS', '50000'))
        max_bytes = int(os.getenv('OLIST_EXTRACT_PART_BYTES', str(64 * 1024 * 1024)))
        for table in HEADERS:
            query = 'SELECT s.payload FROM source_rows s WHERE s.dataset_version=%s AND s.table_name=%s'
            params = [version, table]
            if table == 'orders':
                if start:
                    query += " AND s.payload->>'order_purchase_timestamp' >= %s"; params.append(start)
                if end:
                    query += " AND s.payload->>'order_purchase_timestamp' < %s"; params.append(end)
            elif table in ['order_items', 'order_payments', 'order_reviews'] and (start or end):
                query += " AND EXISTS (SELECT 1 FROM source_rows o WHERE o.dataset_version=s.dataset_version AND o.table_name='orders' AND o.payload->>'order_id'=s.payload->>'order_id'"
                if start:
                    query += " AND o.payload->>'order_purchase_timestamp' >= %s"; params.append(start)
                if end:
                    query += " AND o.payload->>'order_purchase_timestamp' < %s"; params.append(end)
                query += ')'
            query += ' ORDER BY s.ordinal'
            with conn.cursor(name='extract_' + table) as cursor, tempfile.TemporaryDirectory() as temporary:
                cursor.itersize = 1000
                cursor.execute(query, params)
                for path, count in csv_parts((row[0] for row in cursor), HEADERS[table], temporary, max_rows, max_bytes):
                    key = f'{prefix}/{table}/{path.name}'
                    store.put_file(key, path, immutable=True)
                    entry = store.describe(key, count); entry['table'] = table; files.append(entry)
                    path.unlink()
        result = {"schema_version": 1, "batch_id": bid, "attempt_id": attempt, "dataset_version": version, "dataset_kind": source[0]["dataset_kind"], "interval": [start, end], "files": files,
                  "extraction": {"implementation": "server_cursor_csv_parts_v2", "fetch_rows":1000,
                                 "part_max_rows":max_rows, "part_target_bytes":max_bytes,
                                 "oversized_row_policy":"one CSV record may exceed byte target"}}
        store.verify(result, f"raw/{bid}")
        if fail == "before-raw-commit": raise RuntimeError("Injected failure before raw commit")
        store.json(commit, result, immutable=True)
        if fail == "after-raw-commit": raise RuntimeError("Injected failure after raw commit")
        conn.execute("UPDATE batch_progress SET phase='raw_committed',updated_at=now() WHERE batch_id=%s AND attempt_id=%s", (bid, attempt))
    return result
