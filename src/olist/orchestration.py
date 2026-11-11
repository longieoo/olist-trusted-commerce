"""A small finite-job worker; Airflow never receives Docker-daemon access."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from .source import connect

QUEUE_DDL = """CREATE TABLE IF NOT EXISTS orchestration_jobs (
 id text PRIMARY KEY, command jsonb NOT NULL, state text NOT NULL DEFAULT 'queued',
 started_at timestamptz, finished_at timestamptz, exit_code integer)"""

def worker():
    with connect() as conn:
        conn.execute(QUEUE_DDL)
        conn.execute('ALTER TABLE orchestration_jobs ADD COLUMN IF NOT EXISTS expires_at timestamptz')
        conn.execute("UPDATE orchestration_jobs SET expires_at=now() WHERE state='queued' AND expires_at IS NULL")
    while True:
        with connect() as conn:
            conn.execute("UPDATE orchestration_jobs SET state='expired',finished_at=now() WHERE state='queued' AND expires_at<=now()")
            conn.execute("UPDATE orchestration_jobs SET state='failed',finished_at=now(),exit_code=124 WHERE state='running' AND started_at<now()-interval '20 minutes'")
            row=conn.execute("SELECT id,command FROM orchestration_jobs WHERE state='queued' AND expires_at>now() ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1").fetchone()
            if row: conn.execute("UPDATE orchestration_jobs SET state='running',started_at=now() WHERE id=%s",(row[0],))
        if not row: time.sleep(2); continue
        job_id,command=row
        if not isinstance(command,list) or not command or command[0] not in ['fixture','seed','batch','reconcile','warehouse','dbt','local-publish']:
            code=2
        else:
            from .contracts import digest
            path=Path('evidence/airflow-'+digest(job_id)[:16]+'.log'); path.parent.mkdir(exist_ok=True)
            with path.open('w') as log:
                try: code=subprocess.run([sys.executable,'-m','olist.cli',*command],stdout=log,stderr=subprocess.STDOUT,timeout=900).returncode
                except subprocess.TimeoutExpired: code=124
        with connect() as conn:
            conn.execute("UPDATE orchestration_jobs SET state=%s,finished_at=now(),exit_code=%s WHERE id=%s",('succeeded' if code==0 else 'failed',code,job_id))
