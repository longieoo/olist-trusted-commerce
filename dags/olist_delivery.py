"""Thin DAG: enqueue finite jobs; streaming remains a separate service."""
from datetime import datetime, timedelta
import json
import os
import time
import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator

def enqueue(command, cloud=False, **context):
    if cloud and os.getenv('OLIST_CLOUD_AUTHORIZED') != '1':
        raise AirflowSkipException('Cloud execution not authorized/configured')
    job_id=context['dag_run'].dag_id+'/'+context['dag_run'].run_id+'/'+context['task'].task_id
    conn=psycopg2.connect(host=os.getenv('PGHOST','postgres'),dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS orchestration_jobs (id text PRIMARY KEY,command jsonb NOT NULL,state text NOT NULL DEFAULT 'queued',started_at timestamptz,finished_at timestamptz,exit_code integer)")
            cur.execute("ALTER TABLE orchestration_jobs ADD COLUMN IF NOT EXISTS expires_at timestamptz")
            cur.execute("INSERT INTO orchestration_jobs(id,command,expires_at) VALUES(%s,%s,now()+interval '950 seconds') ON CONFLICT(id) DO NOTHING",(job_id,json.dumps(command)))
            cur.execute('SELECT command FROM orchestration_jobs WHERE id=%s FOR UPDATE',(job_id,))
            if cur.fetchone()[0] != command:
                raise AirflowException('Existing job command differs; start a new DAG run')
            cur.execute("UPDATE orchestration_jobs SET state='queued',expires_at=now()+interval '950 seconds' WHERE id=%s AND state IN ('failed','expired')",(job_id,))
        conn.commit()
        for _ in range(190):
            with conn.cursor() as cur:
                cur.execute('SELECT state,exit_code FROM orchestration_jobs WHERE id=%s',(job_id,)); state,code=cur.fetchone()
            conn.commit()
            if state=='succeeded': return {'exit_code':code}
            if state in ('failed','expired'): raise AirflowException(f'Finite job {state} with exit code {code}; see evidence/airflow logs')
            time.sleep(5)
        with conn.cursor() as cur:
            cur.execute("UPDATE orchestration_jobs SET state='expired',finished_at=now() WHERE id=%s AND state='queued'",(job_id,))
        conn.commit()
        raise AirflowException('Wait deadline exceeded; queued job expired, running job has a 900-second subprocess limit')
    finally: conn.close()

with DAG('olist_delivery',start_date=datetime(2026,1,1),schedule=None,catchup=False,max_active_runs=1,default_args={'retries':1,'retry_delay':timedelta(seconds=15)},tags=['olist','fixture']) as dag:
    fixture=PythonOperator(task_id='fixture',python_callable=enqueue,op_kwargs={'command':['fixture']})
    seed=PythonOperator(task_id='seed',python_callable=enqueue,op_kwargs={'command':['seed']})
    batch=PythonOperator(task_id='batch',python_callable=enqueue,op_kwargs={'command':['batch']})
    # Loader arguments are explicit. Obtain the stable batch ID from the committed fixture manifest.
    warehouse=PythonOperator(task_id='warehouse',python_callable=enqueue,op_kwargs={'command':['warehouse','--directory','data/fixtures','--cloud-authorized'],'cloud':True})
    dbt=PythonOperator(task_id='dbt',python_callable=enqueue,op_kwargs={'command':['dbt','--directory','data/fixtures'],'cloud':True})
    fixture >> seed >> batch >> warehouse >> dbt


with DAG('olist_real_delivery', start_date=datetime(2026,1,1), schedule=None,
         catchup=False, max_active_runs=1,
         default_args={'retries':1,'retry_delay':timedelta(seconds=15)},
         tags=['olist','real']) as real_dag:
    real_seed=PythonOperator(task_id='seed',python_callable=enqueue,
        op_kwargs={'command':['seed','--directory','data/real','--kind','real_olist']})
    real_batch=PythonOperator(task_id='batch',python_callable=enqueue,
        op_kwargs={'command':['batch','--directory','data/real']})
    real_warehouse=PythonOperator(task_id='warehouse',python_callable=enqueue,
        op_kwargs={'command':['warehouse','--directory','data/real','--cloud-authorized'],'cloud':True})
    real_dbt=PythonOperator(task_id='dbt',python_callable=enqueue,
        op_kwargs={'command':['dbt','--directory','data/real'],'cloud':True})
    real_seed >> real_batch >> real_warehouse >> real_dbt

with DAG('olist_replay_backfill', start_date=datetime(2026,1,1), schedule=None,
         catchup=False, max_active_runs=1,
         params={'session':'real_20261005','run':'real_20261005'},
         default_args={'retries':1,'retry_delay':timedelta(seconds=15)},
         tags=['olist','finite-recovery']) as backfill_dag:
    recovery=PythonOperator(task_id='reconcile',python_callable=enqueue,
        op_kwargs={'command':['reconcile','--session','{{ params.session }}','--run','{{ params.run }}']})
    replay_warehouse=PythonOperator(task_id='warehouse',python_callable=enqueue,
        op_kwargs={'command':['warehouse','--session','{{ params.session }}','--cloud-authorized'],'cloud':True})
    replay_dbt=PythonOperator(task_id='dbt',python_callable=enqueue,
        op_kwargs={'command':['dbt','--directory','data/real'],'cloud':True})
    recovery >> replay_warehouse >> replay_dbt

# Local end-to-end variant: no cloud task and no cloud credentials required.
with DAG('olist_local_delivery', start_date=datetime(2026,1,1), schedule=None,
         catchup=False, max_active_runs=1,
         default_args={'retries':1,'retry_delay':timedelta(seconds=15)},
         tags=['olist','local','real']) as local_dag:
    local_seed=PythonOperator(task_id='seed',python_callable=enqueue,
        op_kwargs={'command':['seed','--directory','data/real','--kind','real_olist']})
    local_batch=PythonOperator(task_id='batch',python_callable=enqueue,
        op_kwargs={'command':['batch','--directory','data/real']})
    local_publish=PythonOperator(task_id='publish',python_callable=enqueue,
        op_kwargs={'command':['local-publish','--directory','data/real']})
    local_seed >> local_batch >> local_publish
