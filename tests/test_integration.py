import json
from pathlib import Path
import pytest
from olist.storage import Store, batch_id
from olist.source import extract
from olist.batch import process

@pytest.mark.integration
def test_immutable_object_publication():
    import uuid
    store=Store(); key='tests/immutable/'+uuid.uuid4().hex
    store.put(key,b'first',immutable=True); store.put(key,b'first',immutable=True)
    with pytest.raises(ValueError): store.put(key,b'second',immutable=True)
    assert store.get(key)==b'first'

@pytest.mark.integration
def test_publication_failures_and_bounded_replay(monkeypatch):
    import uuid
    monkeypatch.setenv('S3_BUCKET', 'olist-test-' + uuid.uuid4().hex)
    version=json.loads(Path('data/fixtures/manifest.json').read_text())['dataset_version']; store=Store()
    start,end='2018-01-01 00:00:00','2018-02-01 00:00:00'; bid=batch_id(version,start,end)
    # Each invocation exercises every fault boundary in a fresh isolated bucket.
    if not store.maybe(f'raw/{bid}/commit.json'):
        with pytest.raises(RuntimeError): extract(version,start,end,'before-raw-commit')
        assert store.maybe(f'raw/{bid}/commit.json') is None
        assert store.keys(f'raw/{bid}/attempts/')
        with pytest.raises(RuntimeError): extract(version,start,end,'after-raw-commit')
    raw=extract(version,start,end)
    with pytest.raises(RuntimeError): process(raw,'before-curated-commit')
    assert store.maybe(f'curated/{bid}/commit.json') is None
    orphan_parts=store.keys(f'curated/{bid}/attempts/')
    assert orphan_parts
    with pytest.raises(RuntimeError): process(raw,'after-curated-commit')
    committed=process(raw)
    assert committed['accounting']['orders']['accepted']==2
    assert committed['accounting']['order_items']['accepted']==4
    assert all(f['key'] not in orphan_parts for f in committed['files'])
    Path('evidence/publication-recovery.json').write_text(json.dumps({'status':'passed','batch_id':bid,'interval':[start,end],'failure_points':['before-raw-commit','after-raw-commit','before-curated-commit','after-curated-commit'],'accepted_orders':2,'accepted_items':4,'orphan_objects_ignored':len(orphan_parts)},indent=2))
