"""Fail-closed accounting and rejection-rate gates for committed batch evidence."""
from .contracts import HEADERS

def evaluate(manifest, max_reject_rate=0.02):
    if not 0 <= max_reject_rate <= 1:
        raise ValueError('Reject threshold must be between zero and one')
    accounting = manifest.get('accounting', {})
    failures = []; rows = []
    for table in HEADERS:
        counts = accounting.get(table)
        if not isinstance(counts, dict) or any(type(counts.get(k)) is not int or counts[k] < 0 for k in ['raw', 'accepted', 'rejected', 'duplicate']):
            failures.append({'table': table, 'rule': 'missing_or_invalid_counts'}); continue
        rate = counts['rejected'] / counts['raw'] if counts['raw'] else 0
        if counts['raw'] != sum(counts[k] for k in ['accepted', 'rejected', 'duplicate']):
            failures.append({'table': table, 'rule': 'accounting_mismatch'})
        if rate > max_reject_rate:
            failures.append({'table': table, 'rule': 'reject_rate_exceeded'})
        rows.append({'table': table, **counts, 'reject_rate': rate})
    return {'status': 'failed' if failures else 'passed', 'dataset_version': manifest.get('dataset_version'),
            'max_reject_rate': max_reject_rate, 'tables': rows, 'failures': failures,
            'scope': 'Accounting evidence only; does not verify storage bytes or source completeness'}
