"""Bind test results to the exact implementation and XML that were exercised."""
import hashlib
from pathlib import Path

def code_hashes():
    files=[p for directory in ['src','sql','tests','app','dags','scripts','infra','.github']
           for p in Path(directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts]
    files += [p for pattern in ['compose*.yaml','pyproject.toml','requirements*'] for p in Path('.').glob(pattern) if p.is_file()]
    return {p.as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))}
