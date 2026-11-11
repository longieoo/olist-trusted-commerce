"""Credential-free lifecycle records; exception messages are deliberately excluded."""
import json
import time
import uuid
from pathlib import Path
from contextlib import contextmanager
from .contracts import utc_now

@contextmanager
def operation(command, directory='evidence/runs'):
    started = time.perf_counter(); run_id = uuid.uuid4().hex
    record = {'run_id': run_id, 'command': command, 'started_at': utc_now(), 'status': 'running'}
    try:
        yield record
    except BaseException as error:
        if isinstance(error, SystemExit) and error.code in [None, 0]:
            record['status'] = 'succeeded'
        else:
            record.update(status='failed', error_type=type(error).__name__)
        raise
    else:
        record['status'] = 'succeeded'
    finally:
        record.update(finished_at=utc_now(), elapsed_seconds=time.perf_counter()-started)
        root = Path(directory); root.mkdir(parents=True, exist_ok=True)
        target = root / (run_id + '.json'); temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(record, indent=2), encoding='utf-8'); temporary.replace(target)
