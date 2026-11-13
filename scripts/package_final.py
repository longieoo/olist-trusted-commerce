"""Build a deterministic public source ZIP from tracked working-tree files.

The archive records bytes on disk, including intentional uncommitted edits.
Untracked files are excluded. Stage new source files with Git before packing.
"""
import argparse
import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from check_release import release_files, scan_files


def verify_archive(path):
    with ZipFile(path) as archive:
        if archive.testzip() is not None:
            raise ValueError('Archive CRC check failed')
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError('Duplicate archive entries')
        manifest = json.loads(archive.read('olist-trusted-commerce/MANIFEST.json'))
        expected = {'olist-trusted-commerce/' + row['path'] for row in manifest}
        if set(names) != expected | {'olist-trusted-commerce/MANIFEST.json'}:
            raise ValueError('Archive inventory mismatch')
        for row in manifest:
            payload = archive.read('olist-trusted-commerce/' + row['path'])
            if len(payload) != row['bytes'] or hashlib.sha256(payload).hexdigest() != row['sha256']:
                raise ValueError('Archive hash mismatch: ' + row['path'])
    return len(manifest)


def package(root, target):
    root, target = Path(root).resolve(), Path(target).resolve()
    files = [p for p in release_files(root) if p != target and p.name != 'MANIFEST.json']
    if not files:
        raise ValueError('No tracked release files; stage source files with Git first')
    scan_files(root, files)
    payloads = {p.relative_to(root).as_posix(): p.read_bytes() for p in files}
    manifest = [{'path': name, 'bytes': len(blob), 'sha256': hashlib.sha256(blob).hexdigest()}
                for name, blob in sorted(payloads.items())]
    payloads['MANIFEST.json'] = (json.dumps(manifest, indent=2) + '\n').encode()
    target.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(target, 'w', ZIP_DEFLATED, compresslevel=6) as archive:
        for name, blob in sorted(payloads.items()):
            entry = ZipInfo('olist-trusted-commerce/' + name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = ZIP_DEFLATED
            entry.create_system = 3
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, blob, compresslevel=6)
    verify_archive(target)
    report = {'archive': target.name, 'files': len(payloads), 'bytes': target.stat().st_size,
              'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
              'source': 'Tracked working-tree bytes; excludes untracked files and runtime/private data',
              'scope': 'Packaging checks only; no service integration or production claim'}
    target.with_suffix('.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, help='Default: <root>/dist/olist-trusted-commerce.zip')
    args = parser.parse_args()
    print(json.dumps(package(args.root, args.output or args.root / 'dist/olist-trusted-commerce.zip'), indent=2))


if __name__ == '__main__':
    main()
