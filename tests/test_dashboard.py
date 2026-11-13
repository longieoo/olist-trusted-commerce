"""First-clone dashboard behavior using local publications, without external services."""
from pathlib import Path

from streamlit.testing.v1 import AppTest

from olist.local_warehouse import publish, resolve_publication
from test_local_warehouse import source


APP = Path(__file__).resolve().parents[1] / 'app' / 'dashboard.py'
FIXTURE_LABEL = 'Fixture tổng hợp · dữ liệu kiểm thử'


def test_first_run_explains_fixture_setup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('OLIST_LOCAL_ROOT', raising=False)
    app = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not app.exception
    assert 'demo-fixture' in app.info[0].value
    assert not app.metric


def test_fixture_only_defaults_to_truthfully_labelled_source(source, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('OLIST_LOCAL_ROOT', raising=False)
    _, manifest, store = source
    publish(manifest['batch_id'], 'data/local-fixture', store=store)
    app = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not app.exception
    assert app.sidebar.selectbox[0].value == FIXTURE_LABEL
    assert app.metric[0].value == '5'
    assert any('không phải dữ liệu giao dịch Olist thực tế' in info.value for info in app.info)
    app.text_input[0].set_value('missing-order').run()
    assert not app.exception
    assert any('Không tìm thấy order' in info.value for info in app.info)


def test_source_switch_checks_publication_and_never_falls_back(source, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('OLIST_LOCAL_ROOT', raising=False)
    _, manifest, store = source
    publish(manifest['batch_id'], 'data/local-fixture', store=store)
    store.manifest['dataset_kind'] = 'real_olist'
    publish(manifest['batch_id'], 'data/local', store=store)
    app = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not app.exception
    assert app.sidebar.selectbox[0].value == 'Olist lịch sử'
    app.sidebar.selectbox[0].select(FIXTURE_LABEL).run()
    assert not app.exception and app.metric[0].value == '5'
    # Force a cache miss with a changed pointer. The selected source must fail closed,
    # even though the historical publication remains valid and could be displayed.
    path, receipt = resolve_publication('data/local-fixture')
    receipt['published_at'] = '2026-11-13T00:00:00+00:00'
    import json
    Path('data/local-fixture/current.json').write_text(json.dumps(receipt), encoding='utf-8')
    path.write_bytes(b'corrupted database')
    app.run()
    assert not app.exception
    assert any('Không thể xác minh' in error.value for error in app.error)
    assert not app.metric
