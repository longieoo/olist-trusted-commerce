"""Bounded I/O shared by inventory, extraction and publication."""
import hashlib
import io
from pathlib import Path

class BodyReader(io.RawIOBase):
    """IO adapter with its own lifecycle, even when HTTP closes itself at EOF."""
    def __init__(self, body):
        super().__init__(); self.body=body
    def readable(self): return True
    def readinto(self, buffer):
        content=self.body.read(len(buffer));buffer[:len(content)]=content
        return len(content)
    def close(self):
        if not self.closed: self.body.close()
        super().close()

def hash_file(path, chunk_bytes=1024 * 1024):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(chunk_bytes), b''):
            digest.update(chunk)
    return digest.hexdigest()

def csv_parts(rows, headers, directory, max_rows=50000, max_bytes=64 * 1024 * 1024):
    """Yield closed CSV files; at most one row can exceed the byte target."""
    import csv
    if type(max_rows) is not int or max_rows < 1 or type(max_bytes) is not int or max_bytes < 1:
        raise ValueError('Positive integer CSV part limits required')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stream = None; index = 0; any_rows = False
    try:
        for row in rows:
            if stream is None:
                path = directory / f'part-{index:05d}.csv'
                stream = path.open('w', encoding='utf-8', newline='')
                writer = csv.DictWriter(stream, fieldnames=headers)
                writer.writeheader()
                count = 0
            writer.writerow(row)
            any_rows = True
            count += 1
            if count >= max_rows or stream.tell() >= max_bytes:
                stream.close(); stream = None
                yield path, count
                index += 1
        if stream is not None:
            stream.close(); stream = None
            yield path, count
        elif not any_rows:
            path = directory / 'part-00000.csv'
            with path.open('w', encoding='utf-8', newline='') as empty:
                csv.DictWriter(empty, fieldnames=headers).writeheader()
            yield path, 0
    finally:
        if stream is not None:
            stream.close()
