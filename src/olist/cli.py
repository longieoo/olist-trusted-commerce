import argparse
import json
import sys
from pathlib import Path

def main():
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("doctor")
    sub.add_parser("worker")
    f = sub.add_parser("fixture"); f.add_argument("--directory", default="data/fixtures")
    s = sub.add_parser("seed"); s.add_argument("--directory", default="data/fixtures"); s.add_argument("--kind", choices=["test_fixtures", "real_olist"], default="test_fixtures")
    b = sub.add_parser("batch"); b.add_argument("--directory", default="data/fixtures"); b.add_argument("--start"); b.add_argument("--end"); b.add_argument("--fail")
    t = sub.add_parser("test"); t.add_argument("--integration", action="store_true")
    sub.add_parser("smoke")
    sub.add_parser("benchmark-smoke")
    scale = sub.add_parser('portfolio-benchmark')
    scale.add_argument('--sizes', type=int, nargs='+', default=[100000, 1000000])
    scale.add_argument('--repetitions', type=int, default=3)
    gate = sub.add_parser('quality-gate')
    gate.add_argument('--manifest', required=True)
    gate.add_argument('--max-reject-rate', type=float, default=0.02)
    sub.add_parser("stream-smoke")
    r = sub.add_parser("replay"); r.add_argument("--directory", default="data/fixtures"); r.add_argument("--run", default="demo"); r.add_argument("--origin", default="2026-09-30T00:00:00+00:00"); r.add_argument("--speed", type=float, default=86400); r.add_argument("--rate", type=float, default=20); r.add_argument("--duplicates", type=int, default=2); r.add_argument("--malformed", type=int, default=1); r.add_argument("--out-of-order", action="store_true"); r.add_argument("--continuous", action="store_true")
    r.add_argument("--limit-orders", type=int, help="Bound replay to the earliest valid orders; recorded in its manifest")
    st = sub.add_parser("stream"); st.add_argument("--session", default="demo"); st.add_argument("--continuous", action="store_true")
    rec = sub.add_parser("reconcile"); rec.add_argument("--session", default="demo"); rec.add_argument("--run", default="demo")
    w = sub.add_parser("warehouse"); w.add_argument("--batch-id"); w.add_argument("--directory"); w.add_argument("--session"); w.add_argument("--fail"); w.add_argument("--cloud-authorized", action="store_true")
    d = sub.add_parser("dbt"); d.add_argument("--directory")
    sub.add_parser("export")
    local = sub.add_parser('local-publish')
    local_source = local.add_mutually_exclusive_group(required=True)
    local_source.add_argument('--batch-id')
    local_source.add_argument('--directory')
    local.add_argument('--root', default='data/local')
    local.add_argument('--max-reject-rate', type=float, default=0.02)
    local.add_argument('--fail', choices=['before-publication'])
    init = sub.add_parser('changes-init')
    init.add_argument('--source-root',default='data/local')
    init.add_argument('--root',default='data/simulation')
    init.add_argument('--namespace',default='portfolio-demo')
    changes = sub.add_parser('changes-apply')
    changes.add_argument('--root',default='data/simulation')
    changes.add_argument('--file',required=True)
    changes.add_argument('--fail',choices=['before-commit','after-commit'])
    pub = sub.add_parser('changes-publish'); pub.add_argument('--root',default='data/simulation')
    models = sub.add_parser('dbt-local-export'); models.add_argument('--root',default='data/runtime/dbt-local')
    cdc_setup = sub.add_parser('cdc-setup'); cdc_setup.add_argument('--root',default='data/cdc')
    cdc_setup.add_argument('--source-root',default='data/local-fixture')
    cdc_consume = sub.add_parser('cdc-consume'); cdc_consume.add_argument('--root',default='data/cdc')
    cdc_consume.add_argument('--seconds',type=int,default=20)
    a = p.parse_args()
    if a.cmd == 'cdc-setup':
        from .cdc import setup
        result=setup(a.root,a.source_root)
    elif a.cmd == 'cdc-consume':
        from .cdc import consume
        result=consume(a.root,a.seconds)
    elif a.cmd == 'changes-init':
        from .incremental import initialize
        result=initialize(a.source_root,a.root,a.namespace)
    elif a.cmd == 'changes-apply':
        from .incremental import apply
        result=apply(a.root,json.loads(Path(a.file).read_text(encoding='utf-8')),a.fail)
    elif a.cmd == 'changes-publish':
        from .incremental import publish_state
        result=publish_state(a.root)
    elif a.cmd == 'dbt-local-export':
        from .models import export_dbt
        result={'project':export_dbt(a.root)}
    elif a.cmd == 'local-publish':
        from .local_warehouse import publish
        if a.directory:
            from .fixtures import verified_manifest
            from .storage import batch_id
            a.batch_id = batch_id(verified_manifest(a.directory)['dataset_version'])
        result = publish(a.batch_id, a.root, a.max_reject_rate, a.fail)
    elif a.cmd == "worker":
        from .orchestration import worker
        result = worker()
    elif a.cmd == "fixture":
        from .fixtures import write_fixture
        result = write_fixture(a.directory)
    elif a.cmd == "seed":
        from .fixtures import inventory
        from .source import seed
        inventory(a.directory, a.kind); result = seed(a.directory)
    elif a.cmd == "batch":
        from .source import extract
        from .batch import process
        version = json.loads((Path(a.directory) / "manifest.json").read_text())["dataset_version"]
        result = process(extract(version, a.start, a.end, a.fail), a.fail)
    elif a.cmd == "test":
        import pytest
        from .verification import code_hashes
        from .io_utils import hash_file
        from .contracts import utc_now
        name = 'integration-tests' if a.integration else 'tests'
        before = code_hashes()
        code = pytest.main(['-q','-m','integration' if a.integration else 'not integration',f'--junitxml=evidence/{name}.xml'])
        after = code_hashes()
        Path(f'evidence/{name}.receipt.json').write_text(json.dumps({'finished_at':utc_now(),
            'exit_code':int(code),'code_sha256':before,'code_unchanged':before==after,
            'xml_sha256':hash_file(Path(f'evidence/{name}.xml'))},indent=2))
        sys.exit(code if before==after else 1)
    elif a.cmd == "doctor":
        from .ops import doctor
        result = doctor()
    elif a.cmd == "smoke":
        from .ops import smoke
        result = smoke()
    elif a.cmd == "benchmark-smoke":
        from .experiments import benchmark
        result = benchmark()
    elif a.cmd == 'portfolio-benchmark':
        from .portfolio import benchmark
        result = benchmark(a.sizes, a.repetitions)
    elif a.cmd == 'quality-gate':
        from .quality import evaluate
        result = evaluate(json.loads(Path(a.manifest).read_text()), a.max_reject_rate)
        print(json.dumps(result, indent=2))
        if result['status'] != 'passed': raise ValueError('Quality gate failed')
        return
    elif a.cmd == "stream-smoke":
        from .experiments import recovery_experiment
        result = recovery_experiment()
    elif a.cmd == "replay":
        from .replay import produce
        result = produce(a)
    elif a.cmd == "stream":
        from .stream import consume
        result = consume(a)
    elif a.cmd == "reconcile":
        from .stream import reconcile
        result = reconcile(a.session, a.run)
    elif a.cmd == "warehouse":
        if a.directory:
            if a.session: raise ValueError('Directory applies only to historical batches')
            from .fixtures import verified_manifest
            from .storage import batch_id
            expected = batch_id(verified_manifest(a.directory)['dataset_version'])
            if a.batch_id and a.batch_id != expected: raise ValueError('Batch/source version mismatch')
            a.batch_id = expected
        from .warehouse import load
        result = load(a)
    elif a.cmd == "dbt":
        from .warehouse import run_dbt
        result = run_dbt(a.directory)
    elif a.cmd == "export":
        from .ops import export_fixture
        result = export_fixture()
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))

if __name__ == "__main__":
    try:
        from .telemetry import operation
        with operation(sys.argv[1] if len(sys.argv)>1 else 'help'):
            main()
    except Exception as e:
        # Credentials and connection strings are never printed by this boundary.
        message = str(e) if str(e).startswith("Missing variables:") else type(e).__name__
        print(f"FAILED: {message}", file=sys.stderr)
        import os
        if os.getenv("OLIST_DEBUG") == "1":
            import traceback; traceback.print_exc()
        sys.exit(1)
