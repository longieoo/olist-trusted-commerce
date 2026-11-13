"""Safety of repeat setup and a failed verification on a previously green lab."""
import importlib.util
import json
from pathlib import Path
import subprocess
import pytest

spec = importlib.util.spec_from_file_location('lab', Path(__file__).resolve().parents[1]/'scripts/lab.py')
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


def test_setup_never_overwrites_existing_credentials(tmp_path):
    (tmp_path/'.env.example').write_text('PASSWORD=replace-with-random-local-password')
    lab.initialize_env(tmp_path)
    original = (tmp_path/'.env').read_bytes()
    assert b'replace-with' not in original
    lab.initialize_env(tmp_path)
    assert (tmp_path/'.env').read_bytes() == original


def test_preflight_failure_invalidates_previous_green_report(tmp_path, monkeypatch):
    (tmp_path/'.env.example').write_text('PASSWORD=replace-with-random-local-password')
    lab.initialize_env(tmp_path)
    report = tmp_path/'evidence/current/ACCEPTANCE.json'
    report.write_text(json.dumps({'status': 'passed'}))
    monkeypatch.setattr(lab, 'ROOT', tmp_path)
    monkeypatch.setattr('sys.argv', ['lab.py', 'verify'])
    monkeypatch.chdir(tmp_path)
    def docker_offline(*args, **kwargs):
        raise subprocess.CalledProcessError(1, 'docker info')
    monkeypatch.setattr(lab.subprocess, 'run', docker_offline)
    with pytest.raises(subprocess.CalledProcessError):
        lab.main()
    assert json.loads(report.read_text())['status'] == 'failed'
