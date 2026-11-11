"""Exercise queue cancellation and retry against an isolated PostgreSQL schema."""
import ast
from pathlib import Path
from types import SimpleNamespace
import pytest

@pytest.mark.integration
def test_queue_expiry_retry_and_command_identity(monkeypatch):
    from olist.source import connect
    # Execute the real enqueue function without requiring Airflow in the Spark image.
    tree=ast.parse(Path('dags/olist_delivery.py').read_text())
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='enqueue')
    import json,os
    class QueueError(Exception): pass
    class Adapter:
        def connect(self, **kwargs):
            conn=connect()
            conn.execute('SET search_path TO queue_audit')
            conn.commit()
            return conn
    # psycopg3 cursor has the same execute/fetch/context interface used by enqueue.
    namespace={'json':json,'os':os,'psycopg2':Adapter(),'AirflowException':QueueError,'AirflowSkipException':QueueError}
    calls=[]
    def advance(seconds):
        with connect() as conn:
            conn.execute('SET search_path TO queue_audit')
            row=conn.execute('SELECT id,state FROM orchestration_jobs').fetchone()
            calls.append(row[1])
            if len(calls)>190:
                conn.execute("UPDATE orchestration_jobs SET state='succeeded',exit_code=0 WHERE state='queued'")
    namespace['time']=SimpleNamespace(sleep=advance)
    exec(compile(ast.Module(body=[function],type_ignores=[]),'enqueue-under-test','exec'),namespace)
    context={'dag_run':SimpleNamespace(dag_id='audit',run_id='run'),'task':SimpleNamespace(task_id='job')}
    with connect() as conn: conn.execute('CREATE SCHEMA queue_audit')
    try:
        with pytest.raises(QueueError,match='queued job expired'):
            namespace['enqueue'](['seed'],**context)
        with connect() as conn:
            state=conn.execute("SELECT state FROM queue_audit.orchestration_jobs WHERE id='audit/run/job'").fetchone()[0]
            assert state=='expired'
        assert namespace['enqueue'](['seed'],**context)=={'exit_code':0}
        with pytest.raises(QueueError,match='command differs'):
            namespace['enqueue'](['warehouse'],**context)
    finally:
        with connect() as conn: conn.execute('DROP SCHEMA queue_audit CASCADE')
