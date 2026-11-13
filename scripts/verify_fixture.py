"""Accept only current fixture, dbt, CDC and test receipts; no historical inputs."""
import json
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as ET
import duckdb
from olist.contracts import utc_now
from olist.io_utils import hash_file
from olist.local_warehouse import resolve_publication, atomic_json
from olist.verification import code_hashes


def main():
    current = code_hashes()
    tests = {}
    for name in ('tests', 'integration-tests'):
        receipt = json.loads(Path(f'evidence/{name}.receipt.json').read_text())
        assert receipt['exit_code'] == 0 and receipt['code_unchanged']
        assert receipt['code_sha256'] == current, f'Stale {name} evidence'
        xml = Path(f'evidence/{name}.xml')
        assert hash_file(xml) == receipt['xml_sha256']
        tree = ET.parse(xml)
        assert not any(tree.findall('.//' + tag) for tag in ('failure', 'error', 'skipped'))
        tests[name] = len(tree.findall('.//testcase'))
        assert tests[name] > 0
    fixture_db, fixture = resolve_publication('data/local-fixture')
    assert fixture['dataset_kind'] == 'test_fixtures'
    compared = 0
    with duckdb.connect(str(fixture_db), read_only=True) as conn:
        facts = dict((row[0], row[1:]) for row in conn.execute(
            'SELECT order_id,merchandise_brl,freight_brl,payment_brl FROM fact_orders').fetchall())
        for oid, actual in facts.items():
            items = conn.execute('SELECT price,freight_value FROM silver_order_items WHERE order_id=?', [oid]).fetchall()
            payments = conn.execute('SELECT payment_value FROM silver_order_payments WHERE order_id=?', [oid]).fetchall()
            expected = (sum((Decimal(str(r[0])) for r in items), Decimal(0)),
                        sum((Decimal(str(r[1])) for r in items), Decimal(0)),
                        sum((Decimal(str(r[0])) for r in payments), Decimal(0)))
            assert actual == expected, oid
            compared += 1
    assert compared == 5
    reports = {}
    for filename, root in [('upgrade-dbt.json', 'data/local-fixture'), ('upgrade-cdc.json', 'data/cdc')]:
        report = json.loads((Path('evidence')/filename).read_text())
        assert report['status'] == 'passed' and report['code_sha256'] == current
        assert report['generation'] == resolve_publication(root)[1]['generation']
        reports[filename] = {'generation': report['generation'], 'checked_at': report['checked_at']}
    result = {'status': 'passed', 'checked_at': utc_now(), 'tests': tests,
              'fixture_generation': fixture['generation'], 'orders_reconciled': compared,
              'reports': reports, 'code_sha256': current,
              'scope': 'Fixture source-to-publication, independent Decimal oracle, dbt parity and synthetic PostgreSQL WAL CDC'}
    Path('evidence/current').mkdir(parents=True, exist_ok=True)
    atomic_json(Path('evidence/current/fixture.json'), result)
    print(json.dumps({k:v for k,v in result.items() if k != 'code_sha256'}, indent=2))


if __name__ == '__main__':
    main()
