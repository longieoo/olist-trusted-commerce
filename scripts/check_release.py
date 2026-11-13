"""Dependency-free source checks; no historical CSVs or running services needed."""
import argparse
import ast
import csv
import hashlib
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote


EXCLUDED_PARTS = {'.git', '.venv', '__pycache__', '.pytest_cache', 'dist', 'work',
                  'logs', 'target', 'dbt_packages', 'evidence-imported', 'powerbi'}
EXCLUDED_NAMES = {'.env', 'profiles.yml', '.user.yml', 'connector-secret.json'}
EXCLUDED_SUFFIXES = {'.pyc', '.duckdb', '.wal', '.sqlite', '.db', '.log', '.zip', '.7z', '.pem', '.key'}
SOURCE_FOLDERS = {'src', 'sql', 'tests', 'app', 'dags', 'scripts', 'infra', '.github', 'docs', 'evidence', 'dbt', 'data'}
ROOT_FILES = {'README.md', 'LICENSE', 'LICENSE.md', 'NOTICE', 'SECURITY.md', 'CONTRIBUTING.md',
              '.gitignore', '.gitattributes', '.dockerignore', '.env.example', 'Makefile',
              'pyproject.toml', 'requirements.lock', 'requirements-dbt.in', 'MANIFEST.json'}
SECRET = re.compile(rb'gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|'
                    rb'AKIA[0-9A-Z]{16}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----')
LOCAL_PATH = re.compile(rb'(?i)[a-z]:[\\/]+Users[\\/]+[^\s"<>]+|/(?:Users|home)/[a-z0-9_.-]+/')


def public_path(relative):
    relative = Path(relative)
    if relative.is_absolute() or '..' in relative.parts:
        return False
    if any(part in EXCLUDED_PARTS or part.endswith('.egg-info') for part in relative.parts):
        return False
    if relative.name in EXCLUDED_NAMES or relative.name.startswith('.env.') and relative.name != '.env.example':
        return False
    if relative.suffix.lower() in EXCLUDED_SUFFIXES:
        return False
    if len(relative.parts) == 1 and relative.name not in ROOT_FILES and not re.fullmatch(r'compose(?:[.-][a-z0-9_-]+)?\.ya?ml', relative.name):
        return False
    if len(relative.parts) > 1 and relative.parts[0] not in SOURCE_FOLDERS:
        return False
    if relative.parts[0] == 'data' and relative.parts[:2] != ('data', 'fixtures'):
        return False
    if relative.parts[:2] == ('evidence', 'current'):
        return False
    return True


def release_files(root, include_untracked=False):
    command = ['git', '-C', str(root), 'ls-files', '-z', '--cached']
    if include_untracked:
        command += ['--others', '--exclude-standard']
    paths = subprocess.check_output(command).decode('utf-8').split('\0')
    result = []
    for name in sorted(set(filter(None, paths))):
        path = root / name
        if public_path(name) and path.is_file():
            if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
                raise ValueError('Release symlink is not allowed: ' + name)
            result.append(path)
    return result


def scan_files(root, files):
    errors = []
    for path in files:
        blob = path.read_bytes()
        if SECRET.search(blob):
            errors.append('Possible credential: ' + path.relative_to(root).as_posix())
        if LOCAL_PATH.search(blob):
            errors.append('Private machine path: ' + path.relative_to(root).as_posix())
    if errors:
        raise ValueError('; '.join(errors))


def validate_fixtures(root):
    folder = root / 'data/fixtures'
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    if manifest['dataset_kind'] != 'test_fixtures' or len(manifest['files']) != 9:
        raise ValueError('Expected nine synthetic fixture tables')
    for row in manifest['files']:
        name = row['filename']
        if Path(name).name != name:
            raise ValueError('Invalid fixture path')
        path = folder / name
        blob = path.read_bytes()
        if len(blob) != row['bytes'] or hashlib.sha256(blob).hexdigest() != row['sha256']:
            raise ValueError('Fixture checksum mismatch: ' + name)
        with path.open(encoding='utf-8-sig', newline='') as stream:
            if sum(1 for _ in csv.DictReader(stream)) != row['records']:
                raise ValueError('Fixture record count mismatch: ' + name)
    return len(manifest['files'])


def check(root):
    root = Path(root).resolve()
    files = release_files(root, include_untracked=True)
    scan_files(root, files)
    python_files = [p for p in files if p.suffix == '.py']
    for path in python_files:
        ast.parse(path.read_text(encoding='utf-8-sig'), filename=str(path.relative_to(root)))
    documents = [p for p in files if p.suffix == '.md']
    for path in documents:
        for link in re.findall(r'\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
            target = link.strip('<>').split('#', 1)[0]
            if not target or re.match(r'^[a-z]+:', target, re.I):
                continue
            if not (path.parent / unquote(target)).exists():
                raise ValueError('Broken documentation link: ' + path.relative_to(root).as_posix() + ' -> ' + target)
    return {'status': 'passed', 'python_files': len(python_files), 'documents': len(documents),
            'fixture_tables': validate_fixtures(root), 'secret_matches': 0,
            'scope': 'Source syntax, documentation links, synthetic fixtures and public-release scan; no service checks'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    print(json.dumps(check(args.root), indent=2))


if __name__ == '__main__':
    main()
