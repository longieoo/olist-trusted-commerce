import pytest
from olist.storage import Store

@pytest.mark.integration
def test_streamed_file_publication_conflict_and_hash(tmp_path):
    import uuid
    from olist.io_utils import hash_file
    store=Store();key='tests/file/'+uuid.uuid4().hex
    path=tmp_path/'input.bin';path.write_bytes(b'first'*10000)
    store.put_file(key,path,immutable=True);store.put_file(key,path,immutable=True)
    actual=store.describe(key)
    assert actual['bytes']==path.stat().st_size and actual['sha256']==hash_file(path)
    path.write_bytes(b'different')
    with pytest.raises(ValueError,match='Immutable object conflict'):
        store.put_file(key,path,immutable=True)
    assert store.get(key)==b'first'*10000
