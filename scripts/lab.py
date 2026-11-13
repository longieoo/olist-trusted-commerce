"""Portable, fixture-first local lab. Requires Python 3.10+ and Docker Compose v2."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]


def initialize_env(root=ROOT):
    path = root / '.env'
    if not path.exists():
        text = (root / '.env.example').read_text(encoding='utf-8')
        text = text.replace('replace-with-random-local-password', secrets.token_hex(32))
        # Exclusive creation prevents an accidental overwrite of existing credentials.
        with path.open('x', encoding='utf-8') as stream:
            stream.write(text)
        if os.name != 'nt':
            path.chmod(0o600)
    (root / 'evidence' / 'current').mkdir(parents=True, exist_ok=True)


def compose(project):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', project):
        raise ValueError('Project must use lowercase letters, digits, hyphens or underscores')
    return ['docker', 'compose', '-p', project, '-f', 'compose.yaml',
            '-f', 'infra/compose.clean.yaml', '-f', 'infra/compose.cdc.yaml']


def source_hashes():
    from importlib.util import spec_from_file_location, module_from_spec
    spec = spec_from_file_location('verification', ROOT/'src/olist/verification.py')
    module = module_from_spec(spec); spec.loader.exec_module(module)
    return module.code_hashes()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'demo', 'verify', 'status', 'down'])
    parser.add_argument('--project', default=os.getenv('OLIST_PROJECT', 'olist-lab'))
    args = parser.parse_args()
    os.chdir(ROOT)
    dc = compose(args.project)
    initialize_env()
    steps = []
    started = datetime.now(timezone.utc).isoformat()
    report_path = ROOT / 'evidence/current/ACCEPTANCE.json'
    if args.action == 'verify':
        # Never leave an earlier green report in place during a failed run.
        report_path.write_text(json.dumps({'status': 'running', 'started_at': started}), encoding='utf-8')

    def run(label, *cmd):
        print('\n' + label, flush=True)
        subprocess.run([*dc, *cmd], check=True)
        steps.append(label)

    try:
        subprocess.run(['docker', 'info', '--format', '{{.ServerVersion}}'], check=True)
        subprocess.run(['docker', 'compose', 'version'], check=True)
        before = source_hashes() if args.action == 'verify' else None
        if args.action == 'down':
            run('Stop lab (keep volumes)', '--profile', '*', 'down')
        elif args.action == 'status':
            run('Service status', '--profile', '*', 'ps')
        else:
            run('Build runtime and pinned MinIO source', 'build', 'job', 'minio')
            run('Start storage', 'up', '-d', '--wait', 'postgres', 'minio')
            if args.action in ('demo', 'verify'):
                run('Fixture batch', 'run', '--rm', 'job', 'smoke')
                run('Fixture publication', 'run', '--rm', '--no-deps', 'job', 'local-publish',
                    '--directory', 'data/fixtures', '--root', 'data/local-fixture', '--max-reject-rate', '1')
                run('Start dashboard', '--profile', 'ui', 'up', '-d', 'dashboard')
                print('Dashboard: http://localhost:8510 (synthetic fixture)', flush=True)
            if args.action == 'verify':
                run('Unit and dashboard tests', 'run', '--rm', '--no-deps', 'job', 'test')
                run('Storage and failure recovery integration', 'run', '--rm', 'job', 'test', '--integration')
                run('Build dbt validator', 'build', 'dbt-check')
                run('17 dbt models and parity', 'run', '--rm', '--no-deps', 'dbt-check', '--source-root', 'data/local-fixture')
                run('Start CDC', '--profile', 'stream', '--profile', 'cdc', 'up', '-d', '--wait', 'postgres', 'kafka', 'connect')
                run('Configure synthetic WAL source', 'run', '--rm', '--no-deps', 'job', 'cdc-setup')
                run('CDC outage and replay drill', 'run', '--rm', '--no-deps', '--entrypoint', 'python3', 'job', 'scripts/verify_cdc.py')
                run('Fixture acceptance', 'run', '--rm', '--no-deps', '--entrypoint', 'python3', 'job', 'scripts/verify_fixture.py')
                after = source_hashes()
                if before != after:
                    raise RuntimeError('Source changed during verification; rerun against stable code')
                report = {'status': 'passed', 'started_at': started,
                          'checked_at': datetime.now(timezone.utc).isoformat(), 'steps': steps,
                          'code_sha256': after, 'scope': 'Fresh Docker fixture batch, dashboard, dbt and synthetic WAL CDC; no cloud or historical data required'}
                report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    except (subprocess.CalledProcessError, OSError, RuntimeError) as error:
        if args.action == 'verify':
            report_path.write_text(json.dumps({'status': 'failed', 'started_at': started,
                'completed_steps': steps, 'error': str(error)}, indent=2), encoding='utf-8')
        raise


if __name__ == '__main__':
    main()
