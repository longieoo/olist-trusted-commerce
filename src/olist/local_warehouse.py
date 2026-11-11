"""Verified Parquet -> immutable DuckDB generation -> atomic publication pointer.

Single local writer, full snapshot rebuild. Never describes replay as CDC.
"""
import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .contracts import HEADERS, KEYS, digest
from .io_utils import hash_file
from .quality import evaluate

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql' / 'local_marts.sql'


def reconcile(conn):
    """Per-order comparisons catch cancelling errors hidden by global totals."""
    checks = {}
    for table, keys in KEYS.items():
        cols = ','.join(keys)
        checks['null_key_' + table] = conn.execute(
            f"SELECT count(*) FROM silver_{table} WHERE " + ' OR '.join(k+' IS NULL' for k in keys)
        ).fetchone()[0]
        checks['unique_' + table] = conn.execute(
            f'SELECT count(*) FROM (SELECT {cols} FROM silver_{table} GROUP BY {cols} HAVING count(*)>1)'
        ).fetchone()[0]
    for child, key, parent in [('orders','customer_id','customers'),
            ('order_items','order_id','orders'),('order_items','product_id','products'),
            ('order_items','seller_id','sellers'),('order_payments','order_id','orders'),
            ('order_reviews','order_id','orders')]:
        checks[f'fk_{child}_{key}'] = conn.execute(
            f'SELECT count(*) FROM silver_{child} c LEFT JOIN silver_{parent} p USING ({key}) WHERE p.{key} IS NULL'
        ).fetchone()[0]
    checks['fact_order_grain'] = conn.execute('''SELECT abs(
        (SELECT count(*) FROM fact_orders)-(SELECT count(*) FROM silver_orders))
        + (SELECT count(*)-count(DISTINCT order_id) FROM fact_orders)''').fetchone()[0]
    checks['per_order_money'] = conn.execute('''WITH i AS (
        SELECT order_id,sum(price) price,sum(freight_value) freight FROM silver_order_items GROUP BY order_id
      ), p AS (SELECT order_id,sum(payment_value) payment FROM silver_order_payments GROUP BY order_id)
      SELECT count(*) FROM fact_orders f
      FULL OUTER JOIN (SELECT o.order_id,coalesce(i.price,0) price,coalesce(i.freight,0) freight,
        coalesce(p.payment,0) payment FROM silver_orders o LEFT JOIN i USING(order_id) LEFT JOIN p USING(order_id)) s
      ON f.order_id=s.order_id WHERE f.order_id IS NULL OR s.order_id IS NULL
      OR f.merchandise_brl IS DISTINCT FROM s.price OR f.freight_brl IS DISTINCT FROM s.freight
      OR f.payment_brl IS DISTINCT FROM s.payment''').fetchone()[0]
    checks['late_denominator'] = conn.execute('''SELECT count(*) FROM fact_orders WHERE
       (late_eligible=0 AND is_late IS NOT NULL) OR (late_eligible=1 AND (is_late IS NULL OR is_late NOT IN (0,1)))''').fetchone()[0]
    if any(checks.values()):
        raise ValueError('Local reconciliation failed: ' + json.dumps(checks))
    return checks


def materialize(conn, paths, manifest):
    """Read only enumerated committed parts; no globbing orphan attempts."""
    for table in HEADERS:
        parts = paths.get(table, [])
        if not parts:
            raise ValueError('Missing table ' + table)
        conn.read_parquet(parts).create('silver_' + table)
        actual = conn.execute(f'SELECT count(*) FROM silver_{table}').fetchone()[0]
        if actual != manifest['accounting'][table]['accepted']:
            raise ValueError('Accepted row accounting mismatch: ' + table)
    from .models import build
    build(conn)
    return reconcile(conn)


def atomic_json(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with tmp.open('w', encoding='utf-8') as f:
            json.dump(value, f, indent=2, ensure_ascii=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def resolve_publication(root, verify=True, pointer_bytes=None):
    root = Path(root).resolve()
    receipt = json.loads(pointer_bytes if pointer_bytes is not None else (root/'current.json').read_bytes())
    path = (root/receipt['database']).resolve()
    if path.parent != root or path.suffix != '.duckdb':
        raise ValueError('Unsafe publication path')
    if verify and hash_file(path) != receipt['database_sha256']:
        raise ValueError('Published warehouse checksum mismatch')
    return path, receipt


def publish(batch_id, root='data/local', max_reject_rate=0.02, fail=None, store=None):
    import duckdb
    from .storage import Store
    if not re.fullmatch('[a-f0-9]{24}', batch_id):
        raise ValueError('Expected a batch ID from the batch command')
    root = Path(root).resolve(); root.mkdir(parents=True, exist_ok=True)
    lock = root/'.writer.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.close(fd)
    try:
        store = store or Store()
        manifest = store.committed(f'curated/{batch_id}/commit.json', f'curated/{batch_id}')
        if manifest.get('batch_id') != batch_id:
            raise ValueError('Manifest identity mismatch')
        gate = evaluate(manifest, max_reject_rate)
        if gate['status'] != 'passed':
            raise ValueError('Quality gate failed; no publication')
        from . import models
        generation = digest([manifest, hash_file(SQL_PATH), hash_file(Path(__file__)), hash_file(Path(models.__file__)), max_reject_rate])[:24]
        final = root/f'{generation}.duckdb'
        receipt_path = root/f'{generation}.json'
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if receipt['generation'] != generation or receipt['database'] != final.name or receipt['batch_id'] != batch_id:
                raise ValueError('Existing receipt identity mismatch')
            if not final.exists() or hash_file(final) != receipt['database_sha256']:
                raise ValueError('Existing generation corrupted')
            atomic_json(root/'current.json', receipt)
            return receipt
        with tempfile.TemporaryDirectory(prefix='build-', dir=root) as tmp:
            tmp = Path(tmp); paths = {}; quarantine = []
            for i, entry in enumerate(manifest['files']):
                table = entry['table']
                if table not in HEADERS and table not in {'quarantine_'+t for t in HEADERS}:
                    raise ValueError('Unknown table in manifest')
                local = tmp/f'part-{i}.parquet'
                response = store.client.get_object(Bucket=store.bucket, Key=entry['key'])
                with response['Body'] as source, local.open('wb') as out:
                    for chunk in iter(lambda: source.read(1024*1024), b''):
                        out.write(chunk)
                if hash_file(local) != entry['sha256'] or local.stat().st_size != entry['bytes']:
                    raise ValueError('Downloaded part checksum mismatch')
                import pyarrow.parquet as pq
                if type(entry.get('rows')) is not int or pq.read_metadata(local).num_rows != entry['rows']:
                    raise ValueError('Part row accounting mismatch')
                paths.setdefault(table, []).append(str(local))
            db = tmp/'warehouse.duckdb'
            conn = duckdb.connect(str(db))
            try:
                conn.execute("SET memory_limit='512MB'")
                conn.execute('SET threads=2')
                checks = materialize(conn, paths, manifest)
                conn.execute('CREATE TABLE quality_accounting(table_name VARCHAR,raw BIGINT,accepted BIGINT,rejected BIGINT,duplicate BIGINT)')
                conn.executemany('INSERT INTO quality_accounting VALUES (?,?,?,?,?)', [
                    (t, *(manifest['accounting'][t][k] for k in ['raw','accepted','rejected','duplicate'])) for t in HEADERS])
                conn.execute('CREATE TABLE quarantine_reasons(table_name VARCHAR,reason VARCHAR,rows BIGINT)')
                for t in HEADERS:
                    parts=paths.get('quarantine_'+t, [])
                    if not parts: raise ValueError('Missing quarantine table '+t)
                    conn.read_parquet(parts).create_view('quarantine_input', replace=True)
                    rows=conn.execute('SELECT _reason,count(*) FROM quarantine_input GROUP BY _reason').fetchall()
                    if sum(r[1] for r in rows) != sum(manifest['accounting'][t][k] for k in ['rejected','duplicate']):
                        raise ValueError('Quarantine accounting mismatch')
                    quarantine.extend((t,*r) for r in rows)
                if quarantine: conn.executemany('INSERT INTO quarantine_reasons VALUES (?,?,?)', quarantine)
                conn.execute('DROP VIEW quarantine_input')
                summary=dict(zip(['orders','merchandise_brl','freight_brl','payment_brl','eligible_delivered','late_orders'],
                    conn.execute('SELECT count(*),sum(merchandise_brl),sum(freight_brl),sum(payment_brl),sum(late_eligible),sum(coalesce(is_late,0)) FROM fact_orders').fetchone()))
                summary={k:str(v) if k.endswith('_brl') else v for k,v in summary.items()}
                conn.execute('CHECKPOINT')
            finally:
                conn.close()
            if fail=='before-publication': raise RuntimeError('Injected publication failure')
            os.replace(db, final)
        receipt={'generation':generation,'database':final.name,'database_sha256':hash_file(final),
            'batch_id':batch_id,'dataset_version':manifest['dataset_version'], 'dataset_kind':manifest['dataset_kind'],
            'interval':manifest['interval'],'published_at':datetime.now(timezone.utc).isoformat(),
            'source_reference':'https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce' if manifest['dataset_kind']=='real_olist' else 'src/olist/fixtures.py',
            'mode':'full snapshot from committed curated parts','quality':gate,'checks':checks,'summary':summary}
        atomic_json(receipt_path,receipt)
        atomic_json(root/'current.json',receipt)
        Path('evidence').mkdir(exist_ok=True)
        atomic_json(Path('evidence')/f'local-{batch_id}.json',receipt)
        return receipt
    finally:
        lock.unlink(missing_ok=True)
