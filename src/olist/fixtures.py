import csv
import hashlib
import json
from pathlib import Path
from .contracts import HEADERS, FILES, SOURCE, digest

def write_fixture(directory):
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    def row(table, *values): return dict(zip(HEADERS[table], values))
    data = {
        "customers": [row("customers", "c1", "u1", "00123", "sao paulo", "SP"), row("customers", "c2", "u1", "04567", "rio", "RJ"), row("customers", "c3", "u3", "00123", "sao paulo", "SP")],
        "sellers": [row("sellers", "s1", "00123", "sao paulo", "SP"), row("sellers", "s2", "04567", "rio", "RJ")],
        "products": [row("products", "p1", "books", 10, 20, 1, 100, 10, 2, 10), row("products", "p2", "toys", 10, 20, 1, 200, 20, 4, 20)],
        "orders": [
            row("orders", "o1", "c1", "delivered", "2018-01-01 10:00:00", "2018-01-01 11:00:00", "2018-01-03 10:00:00", "2018-01-06 15:00:00", "2018-01-05 00:00:00"),
            row("orders", "o2", "c2", "delivered", "2018-01-02 10:00:00", "2018-01-02 11:00:00", "2018-01-04 10:00:00", "2018-01-07 18:00:00", "2018-01-07 00:00:00"),
            row("orders", "o3", "c3", "shipped", "2018-02-01 10:00:00", "2018-02-01 11:00:00", "2018-02-03 10:00:00", "", "2018-02-10 00:00:00"),
            row("orders", "o4", "c1", "canceled", "2018-02-02 10:00:00", "", "", "", "2018-02-10 00:00:00"),
            row("orders", "o5", "c3", "delivered", "2018-03-01 10:00:00", "", "", "2018-03-03 12:00:00", ""),
            row("orders", "orphan", "missing", "delivered", "2018-01-01 10:00:00", "", "", "", ""),
            row("orders", "invalid", "c1", "delivered", "bad", "", "", "", ""),
        ],
        "order_items": [row("order_items", "o1", 1, "p1", "s1", "2018-01-04 00:00:00", "10.10", "1.00"), row("order_items", "o1", 2, "p2", "s2", "2018-01-04 00:00:00", "20.20", "2.00"), row("order_items", "o1", 3, "p1", "s1", "2018-01-04 00:00:00", "30.30", "3.00"), row("order_items", "o2", 1, "p2", "s2", "2018-01-05 00:00:00", "50.00", "5.00"), row("order_items", "missing", 1, "p1", "s1", "", "1.00", "0.00"), row("order_items", "o2", 2, "p1", "s1", "", "-1.00", "0.00")],
        "order_payments": [row("order_payments", "o1", 1, "credit_card", 1, "40.00"), row("order_payments", "o1", 2, "voucher", 1, "26.60"), row("order_payments", "o2", 1, "credit_card", 1, "55.00")],
        "order_reviews": [row("order_reviews", "r1", "o1", 2, "late", "first line\nsecond line", "2018-01-07 00:00:00", "2018-01-07 12:00:00"), row("order_reviews", "r2", "o1", 3, "updated", "", "2018-01-08 00:00:00", "2018-01-08 12:00:00"), row("order_reviews", "r3", "o2", 5, "good", "", "2018-01-08 00:00:00", "2018-01-08 13:00:00"), row("order_reviews", "r4", "o2", 9, "bad score", "", "", "")],
        "geolocation": [row("geolocation", "00123", "-23.55", "-46.63", "sao paulo", "SP"), row("geolocation", "00123", "-23.56", "-46.64", "sao paulo", "SP")],
        "category_translation": [row("category_translation", "books", "books"), row("category_translation", "toys", "toys")],
    }
    data["orders"].append(dict(data["orders"][0]))
    data["order_items"].append(dict(data["order_items"][0]))
    for table, rows in data.items():
        with (root / FILES[table]).open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS[table]); writer.writeheader(); writer.writerows(rows)
    return inventory(root, "test_fixtures")

def verified_manifest(directory):
    """Read an existing inventory without rewriting its immutable source identity."""
    root = Path(directory)
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    files = manifest['files']
    if manifest.get('schema_version') != 1 or manifest['dataset_version'] != digest(files):
        raise ValueError('Invalid source manifest identity')
    if {f['table'] for f in files} != set(FILES) or len(files) != len(FILES):
        raise ValueError('Incomplete source inventory')
    for entry in files:
        if entry['filename'] != FILES[entry['table']]:
            raise ValueError('Invalid source filename')
        path = root / entry['filename']
        from .io_utils import hash_file
        if path.stat().st_size != entry['bytes'] or hash_file(path) != entry['sha256']:
            raise ValueError('Source bytes changed after inventory')
    return manifest

def inventory(directory, kind):
    if kind not in ["test_fixtures", "real_olist"]: raise ValueError("Explicit dataset kind required")
    root = Path(directory); files = []
    for table, name in FILES.items():
        path = root / name
        if not path.is_file(): raise ValueError("Missing source file: " + name)
        with path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != HEADERS[table]: raise ValueError("Header mismatch: " + name)
            rows = sum(1 for _ in reader)
        from .io_utils import hash_file
        files.append({"table": table, "filename": name, "sha256": hash_file(path), "records": rows, "bytes": path.stat().st_size})
    version = digest(files)
    manifest = {"schema_version": 1, "dataset_version": version, "dataset_kind": kind, "source_reference": SOURCE if kind == "real_olist" else "src/olist/fixtures.py", "acquisition": "user-provided; date unknown" if kind == "real_olist" else "deterministically generated", "license_reference": SOURCE if kind == "real_olist" else "CC0 project fixture", "files": files}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
