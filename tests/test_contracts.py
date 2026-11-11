import csv
import json
from decimal import Decimal
import pytest
from olist.contracts import money, validate_event, digest
from olist.fixtures import write_fixture, inventory
from olist.batch import row_reason
from olist.replay import reconstruct
from olist.storage import batch_id, Store

def test_fixture_record_counts_and_bytes(tmp_path):
    a = write_fixture(tmp_path); b = write_fixture(tmp_path)
    assert a == b
    assert next(f["records"] for f in a["files"] if f["table"] == "order_reviews") == 4
    with (tmp_path / "olist_customers_dataset.csv").open(newline="") as f:
        rows = list(csv.DictReader(f)); assert rows[0]["customer_zip_code_prefix"] == "00123"
        assert rows[0]["customer_unique_id"] == rows[1]["customer_unique_id"]
    assert rows[0]["customer_id"] != rows[1]["customer_id"]

@pytest.mark.parametrize("value", ["-0.01", "NaN", "Infinity", "1.001", "10000000000000000", "bad"])
def test_invalid_money(value):
    with pytest.raises(ValueError): money(value)

def test_decimal_totals():
    assert sum(map(money, ["10.10", "20.20", "30.30"])) == Decimal("60.60")

def test_ids_are_interval_and_version_scoped():
    assert batch_id("a") == batch_id("a")
    assert len({batch_id("a"), batch_id("b"), batch_id("a", "2018-01-01 00:00:00")}) == 3

def test_replay_contract_and_no_future_status():
    rows = [{"order_id": "o1", "order_status": "delivered", "order_purchase_timestamp": "2018-01-01 10:00:00", "order_delivered_customer_date": "2018-01-03 10:00:00"}]
    events = reconstruct(rows, "v1", "run1", "2026-01-01T00:00:00+00:00", 86400)
    assert len(events) == 2 and events[1]["event_time"] == "2026-01-01T00:00:02+00:00"
    assert "order_status" not in events[0]
    assert events == reconstruct(rows, "v1", "run1", "2026-01-01T00:00:00+00:00", 86400)
    assert events[0]["event_id"] != reconstruct(rows, "v1", "run2", "2026-01-01T00:00:00+00:00", 86400)[0]["event_id"]
    event = {**events[0], "emitted_at": "2026-01-01T00:00:00+00:00"}
    assert validate_event(event) is None
    assert validate_event({**event, "event_id": "tampered"}) == "invalid_event_id"
    assert validate_event({**event, "is_simulated": False}) == "invalid_contract"

def test_manifest_path_and_hash_validation():
    from types import SimpleNamespace
    from io import BytesIO
    store = object.__new__(Store)
    store.client = SimpleNamespace(get_object=lambda **kwargs: {'Body': BytesIO(b'x')})
    store.bucket = 'test'
    import hashlib
    f = {"key": "root/part", "bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest()}
    assert store.verify({"schema_version": 1, "files": [f]}, "root")
    for altered in [{**f, "key": "root/../secret"}, {**f, "sha256": "wrong"}, {**f, "key": "other/part"}]:
        with pytest.raises(ValueError): store.verify({"schema_version": 1, "files": [altered]}, "root")
    with pytest.raises(ValueError): store.verify({"schema_version": 1, "files": [f, f]}, "root")
    for version in [True, 1.0, '1']:
        with pytest.raises(ValueError): store.verify({'schema_version':version,'files':[f]},'root')

def test_missing_timestamps_are_retained():
    row = {"order_id": "o", "customer_id": "c", "order_status": "canceled", "order_purchase_timestamp": "2018-01-01 00:00:00", "order_approved_at": "", "order_delivered_carrier_date": "", "order_delivered_customer_date": ""}
    assert row_reason("orders", row) is None
    row["order_delivered_customer_date"] = "2017-01-01 00:00:00"
    assert row_reason("orders", row) == "invalid_chronology"

@pytest.mark.parametrize('value', ['40', '40.0', '4E1'])
def test_integral_source_representation(value):
    assert row_reason('products', {'product_id':'p','product_weight_g':value}) is None

def test_fractional_and_overflow_integer_rejected():
    assert row_reason('products', {'product_id':'p','product_weight_g':'1.5'}) == 'invalid_integer'
    assert row_reason('products', {'product_id':'p','product_weight_g':'2147483648'}) == 'invalid_integer'

def test_short_numeric_zip_preserves_source_compatibility():
    assert row_reason('customers', {'customer_id':'c','customer_unique_id':'u','customer_zip_code_prefix':'1234'}) is None
    assert row_reason('customers', {'customer_id':'c','customer_unique_id':'u','customer_zip_code_prefix':'123456'}) == 'invalid_zip'

@pytest.mark.parametrize('version', [True, 1.0, '1', None])
def test_event_schema_version_has_integer_type(version):
    events = reconstruct([{'order_id':'o','order_purchase_timestamp':'2018-01-01 00:00:00'}], 'v', 'r', '2026-01-01T00:00:00+00:00', 1)
    event = {**events[0], 'emitted_at':'2026-01-01T00:00:00+00:00'}
    assert validate_event(event) is None
    assert validate_event({**event, 'schema_version':version}) is not None

@pytest.mark.parametrize('kind', [[], {}, 1, True])
def test_invalid_event_type_is_quarantined_instead_of_crashing(kind):
    events = reconstruct([{'order_id':'o','order_purchase_timestamp':'2018-01-01 00:00:00'}], 'v', 'r', '2026-01-01T00:00:00+00:00', 1)
    event = {**events[0], 'emitted_at':'2026-01-01T00:00:00+00:00', 'event_type':kind}
    assert validate_event(event)=='invalid_identity'

def test_replay_bom_and_source_tampering(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from olist.replay import produce
    from olist.contracts import FILES
    write_fixture(tmp_path)
    for table in ['customers','orders']:
        path = tmp_path / FILES[table]
        path.write_bytes(b'\xef\xbb\xbf' + path.read_bytes())
    inventory(tmp_path, 'test_fixtures')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('olist.replay.send_payloads', lambda events, *args: {'acknowledged':len(events)})
    args = SimpleNamespace(directory=tmp_path, duplicates=0, malformed=0, run='bom', origin='2026-01-01T00:00:00+00:00', speed=86400, rate=20, out_of_order=False, continuous=False)
    assert produce(args)['manifest']['event_count'] > 0
    with (tmp_path / FILES['orders']).open('ab') as stream: stream.write(b'\n')
    with pytest.raises(ValueError, match='Source bytes changed'): produce(args)

def test_dbt_binds_source_version_instead_of_stale_environment(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from olist.warehouse import run_dbt
    manifest = write_fixture(tmp_path)
    for key in ['OLIST_CLOUD_AUTHORIZED','SNOWFLAKE_ACCOUNT','SNOWFLAKE_USER','SNOWFLAKE_PASSWORD']:
        monkeypatch.setenv(key, '1')
    monkeypatch.setenv('DBT_DATASET_VERSION', 'a'*64)
    observed = {}
    def fake_run(command, **kwargs):
        observed.update(kwargs['env']); return SimpleNamespace(returncode=0)
    monkeypatch.setattr('olist.warehouse.subprocess.run', fake_run)
    assert run_dbt(tmp_path) == {'exit_code':0}
    assert observed['DBT_DATASET_VERSION'] == manifest['dataset_version']
    assert observed['DBT_DATASET_VERSION'] != 'a'*64
