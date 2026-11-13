"""Public packaging must never leak runtime inputs or misstate archive contents."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from check_release import check, public_path, scan_files, validate_fixtures
from package_final import package, verify_archive


@pytest.mark.parametrize('name', ['.env', '.env.production', 'data/real/orders.csv',
                                  'data/cdc/inbox.sqlite', 'data/runtime/export.json',
                                  'dbt/profiles.yml', 'dbt/target/results.json',
                                  'cache/private.key', '../outside.txt'])
def test_private_and_runtime_paths_excluded(name):
    assert not public_path(name)


def test_synthetic_inputs_and_example_config_allowed():
    assert public_path('data/fixtures/olist_orders_dataset.csv')
    assert public_path('.env.example')


def test_release_scan_rejects_credentials_without_echoing_them(tmp_path):
    path = tmp_path / 'accident.txt'
    secret = 'gh' + 'p_' + 'a' * 36
    path.write_text(secret)
    with pytest.raises(ValueError, match='Possible credential') as failure:
        scan_files(tmp_path, [path])
    assert secret not in str(failure.value)
    path.write_text('C:' + '\\Users\\PrivateUser\\work\\config.json')
    with pytest.raises(ValueError, match='Private machine path'):
        scan_files(tmp_path, [path])


def test_package_uses_tracked_files_and_is_reproducible(tmp_path):
    subprocess.run(['git', 'init', '-q', str(tmp_path)], check=True)
    (tmp_path / 'README.md').write_bytes(b'# Project\n')
    (tmp_path / '.env').write_text('not public')
    (tmp_path / 'data/real').mkdir(parents=True)
    (tmp_path / 'data/real/orders.csv').write_text('private raw data')
    subprocess.run(['git', '-C', str(tmp_path), 'add', '.'], check=True)
    (tmp_path / 'untracked.txt').write_text('not reviewed')
    target = tmp_path / 'dist/source.zip'
    first = package(tmp_path, target)
    second = package(tmp_path, target)
    assert first['sha256'] == second['sha256']
    assert verify_archive(target) == 1
    with ZipFile(target) as archive:
        assert set(archive.namelist()) == {'olist-trusted-commerce/README.md', 'olist-trusted-commerce/MANIFEST.json'}
        manifest = json.loads(archive.read('olist-trusted-commerce/MANIFEST.json'))
        assert manifest[0]['sha256'] == hashlib.sha256(b'# Project\n').hexdigest()


def test_fixture_bytes_are_checked_without_external_data(tmp_path):
    import shutil
    source = Path(__file__).resolve().parents[1] / 'data/fixtures'
    shutil.copytree(source, tmp_path / 'data/fixtures')
    assert validate_fixtures(tmp_path) == 9
    with (tmp_path / 'data/fixtures/olist_orders_dataset.csv').open('ab') as stream:
        stream.write(b'\n')
    with pytest.raises(ValueError, match='Fixture checksum mismatch'):
        validate_fixtures(tmp_path)


def test_archive_verification_detects_changed_content(tmp_path):
    subprocess.run(['git', 'init', '-q', str(tmp_path)], check=True)
    (tmp_path / 'README.md').write_bytes(b'# Original\n')
    subprocess.run(['git', '-C', str(tmp_path), 'add', 'README.md'], check=True)
    original = tmp_path / 'dist/original.zip'
    package(tmp_path, original)
    forged = tmp_path / 'dist/forged.zip'
    with ZipFile(original) as source, ZipFile(forged, 'w') as target:
        for name in source.namelist():
            target.writestr(name, b'# Changed\n' if name.endswith('/README.md') else source.read(name))
    with pytest.raises(ValueError, match='Archive hash mismatch'):
        verify_archive(forged)


def test_source_ci_rejects_forcibly_tracked_env_without_echoing_value(tmp_path):
    subprocess.run(['git', 'init', '-q', str(tmp_path)], check=True)
    secret = 'private-local-password'
    (tmp_path/'.env').write_text('PASSWORD=' + secret)
    subprocess.run(['git', '-C', str(tmp_path), 'add', '.env'], check=True)
    with pytest.raises(ValueError, match='Forbidden tracked release paths') as result:
        check(tmp_path)
    assert secret not in str(result.value)
