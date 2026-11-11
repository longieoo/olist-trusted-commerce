import csv
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from .contracts import EVENTS, digest, source_utc, utc_now

def safe_name(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value): raise ValueError("Invalid run/session name")
    return value

def reconstruct(orders, version, run, origin, speed):
    safe_name(run)
    target = datetime.fromisoformat(origin)
    if target.tzinfo is None or speed <= 0: raise ValueError("Timezone-aware origin and positive speed required")
    source = []
    for order in orders:
        for kind, column in EVENTS.items():
            value = order.get(column)
            if value:
                try: timestamp = source_utc(value)
                except ValueError: continue
                source.append((timestamp, order["order_id"], kind, value))
    source = sorted(set(source))
    if not source: return []
    baseline = min(e[0] for e in source)
    return [{"event_id": digest([version, run, order, kind, value]), "schema_version": 1, "dataset_version": version, "replay_run_id": run, "order_id": order, "event_type": kind, "source_event_time": value, "event_time": (target + (ts - baseline) / speed).isoformat(), "is_simulated": True} for ts, order, kind, value in source]

def producer():
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    config = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")}
    topic = os.getenv("KAFKA_TOPIC", "olist-lifecycle-v1")
    admin = AdminClient(config)
    if topic not in admin.list_topics(timeout=20).topics:
        for future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values(): future.result(20)
    return Producer({**config, "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 30000}), topic

def send_payloads(payloads, journal_name, rate=20):
    safe_name(journal_name)
    if rate <= 0: raise ValueError("rate must be positive")
    payloads = list(payloads)
    root = Path("data/runtime/replay"); root.mkdir(parents=True, exist_ok=True)
    # One local writer per journal. Never silently steal a stale crash lock.
    lock = root / f'{journal_name}.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY); os.close(fd)
    try:
        db = sqlite3.connect(root / f"{journal_name}.sqlite")
        try:
            return _send_journal(db, payloads, journal_name, rate, root)
        finally:
            db.close()
    finally:
        lock.unlink(missing_ok=True)

def _send_journal(db, payloads, journal_name, rate, root):
    db.execute("PRAGMA journal_mode=WAL"); db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY,payload TEXT NOT NULL,ack_topic TEXT,ack_partition INTEGER,ack_offset INTEGER,ack_at TEXT)")
    db.execute('CREATE TABLE IF NOT EXISTS binding(id INTEGER PRIMARY KEY CHECK(id=1), fingerprint TEXT NOT NULL)')
    fingerprint = digest([payloads, os.getenv('KAFKA_BOOTSTRAP','kafka:9092'), os.getenv('KAFKA_TOPIC','olist-lifecycle-v1')])
    previous = db.execute('SELECT fingerprint FROM binding WHERE id=1').fetchone()
    if previous and previous[0] != fingerprint:
        raise ValueError('Replay journal input or destination changed; use a new run name')
    if not previous:
        if db.execute('SELECT count(*) FROM outbox').fetchone()[0]:
            raise ValueError('Legacy replay journal has no input binding; use a new run name')
        db.execute('INSERT INTO binding VALUES(1,?)', (fingerprint,)); db.commit()
    p, topic = producer(); sent = 0
    for seq, payload in enumerate(payloads):
        # Persist emitted_at once before send; resends reuse exact payload and event ID.
        old = db.execute("SELECT payload,ack_offset FROM outbox WHERE seq=?", (seq,)).fetchone()
        if old:
            if old[1] is not None: continue
            encoded = old[0]
        else:
            if isinstance(payload, dict): payload = {**payload, "emitted_at": utc_now()}
            encoded = json.dumps(payload) if isinstance(payload, dict) else payload
            db.execute("INSERT INTO outbox(seq,payload) VALUES(?,?)", (seq, encoded)); db.commit()
        errors = []
        def ack(error, message, n=seq):
            if error: errors.append(str(error)); return
            db.execute("UPDATE outbox SET ack_topic=?,ack_partition=?,ack_offset=?,ack_at=? WHERE seq=?", (message.topic(), message.partition(), message.offset(), utc_now(), n)); db.commit()
        p.produce(topic, key=journal_name.encode(), value=encoded.encode(), on_delivery=ack)
        if p.flush(35) or errors: raise RuntimeError("Kafka acknowledgment failed")
        sent += 1; time.sleep(1 / rate)
    acknowledged = db.execute("SELECT COUNT(*) FROM outbox WHERE ack_offset IS NOT NULL").fetchone()[0]
    return {"sent_this_attempt": sent, "acknowledged": acknowledged, "journal": str(root / f"{journal_name}.sqlite")}

def produce(args):
    from .batch import row_reason
    from .contracts import FILES
    if args.duplicates < 0 or args.malformed < 0: raise ValueError("Injection counts must be nonnegative")
    from .fixtures import verified_manifest
    root = Path(args.directory); source = verified_manifest(root)
    with (root / FILES["orders"]).open(encoding="utf-8-sig", newline="") as f: orders = list(csv.DictReader(f))
    with (root / FILES["customers"]).open(encoding="utf-8-sig", newline="") as f: customers = {r["customer_id"] for r in csv.DictReader(f)}
    orders = [r for r in orders if row_reason("orders", r) is None and r["customer_id"] in customers]
    limit = getattr(args, 'limit_orders', None)
    if limit is not None:
        if limit <= 0: raise ValueError('limit-orders must be positive')
        orders = sorted(orders, key=lambda r: (r['order_purchase_timestamp'], r['order_id']))[:limit]
    events = reconstruct(orders, source["dataset_version"], args.run, args.origin, args.speed)
    manifest = {"schema_version": 1, "replay_run_id": args.run, "dataset_version": source["dataset_version"], "origin": args.origin, "speed": args.speed, "source_timezone_assumption": "fixed UTC-03:00", "ordering": "source timestamp, order ID, event type", "duplicates": args.duplicates, "malformed": args.malformed, "out_of_order": args.out_of_order, "event_count": len(events), "rate": args.rate}
    if limit is not None:
        manifest.update({'limit_orders': limit, 'selected_orders': len(orders), 'selection': 'earliest valid purchase timestamp, then order_id'})
    path = Path(f"data/runtime/replay/{safe_name(args.run)}.json"); path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and json.loads(path.read_text()) != manifest: raise ValueError("Existing run has a different mapping/configuration")
    path.write_text(json.dumps(manifest, indent=2))
    if args.out_of_order and len(events) > 2: events = events[2:] + events[:2]
    events += events[:args.duplicates]; events += ["{malformed"] * args.malformed
    result = send_payloads(events, args.run, args.rate)
    # Continuous mode stays alive; it does not generate fictitious milestones.
    while args.continuous: time.sleep(1)
    return {**result, "manifest": manifest}
