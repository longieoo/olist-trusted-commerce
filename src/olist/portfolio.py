"""Measured local scalability study. Synthetic data never enters historical marts."""
import json
import os
import statistics
import time
import uuid
from pathlib import Path
from .contracts import digest, utc_now

def event_metrics(directory):
    groups = {}; stage_groups = {}
    for path in Path(directory).rglob('*'):
        if not path.is_file() or path.name.startswith('.'): continue
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                event = json.loads(line)
                if event.get('Event') == 'SparkListenerJobStart':
                    group = event.get('Properties', {}).get('spark.jobGroup.id', 'setup')
                    for stage in event['Stage IDs']: stage_groups[stage] = group
                if event.get('Event') != 'SparkListenerTaskEnd': continue
                group = stage_groups.get(event['Stage ID'], 'setup')
                row = groups.setdefault(group, {'tasks':0, 'failed_tasks':0, 'executor_run_ms':0, 'executor_cpu_ns':0,
                                              'memory_spill_bytes':0, 'disk_spill_bytes':0, 'shuffle_read_bytes':0,
                                              'shuffle_write_bytes':0, 'max_task_execution_memory_bytes':0})
                m = event.get('Task Metrics', {})
                row['tasks'] += 1
                row['failed_tasks'] += int(event.get('Task End Reason', {}).get('Reason') != 'Success')
                for out,key in [('executor_run_ms','Executor Run Time'),('executor_cpu_ns','Executor CPU Time'),
                                ('memory_spill_bytes','Memory Bytes Spilled'),('disk_spill_bytes','Disk Bytes Spilled')]:
                    row[out] += m.get(key, 0)
                read = m.get('Shuffle Read Metrics', {})
                row['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                row['shuffle_write_bytes'] += m.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                row['max_task_execution_memory_bytes'] = max(row['max_task_execution_memory_bytes'], m.get('Peak Execution Memory',0))
    return groups

def benchmark(sizes, repetitions):
    from pyspark.sql import functions as F, types as T
    from .batch import spark_session
    if not sizes or any(type(n) is not int or n < 1 or n > 10000000 for n in sizes) or not 1 <= repetitions <= 10:
        raise ValueError('Sizes must be 1..10000000 and repetitions 1..10')
    run = uuid.uuid4().hex[:12]; root = Path('data/runtime/portfolio') / run
    events = root / 'spark-events'; os.environ['OLIST_SPARK_EVENT_DIR'] = str(events)
    spark = spark_session('olist-portfolio-' + run)
    started = time.perf_counter()
    report = {'schema_version':1, 'run_id':run, 'measured_at':utc_now(), 'dataset_kind':'synthetic_scalability',
              'generator_version':1, 'spark_version':spark.version, 'master':spark.sparkContext.master,
              'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'), 'repetitions':repetitions,
              'warmups':1, 'cache_policy':'Spark cache cleared before each trial; OS cache remains warm; order rotated',
              'scope':'Parquet read, synthetic validation predicate and count/sum action; not complete Olist pipeline throughput',
              'timing_scope':'first() action execution; query/DataFrame construction and metadata discovery excluded',
              'cases':[]}
    try:
        for n in sizes:
            path = root / str(n)
            # A wider input makes file scanning real; payload depends on ID and is used by both predicates.
            frame = spark.range(n).select('id', F.concat(F.sha2(F.col('id').cast('string'),256), F.lit('x'*128)).alias('payload'))
            spark.sparkContext.setJobGroup(f'write-{n}','Generate isolated synthetic Parquet')
            frame.repartition(4).write.mode('errorifexists').parquet(str(path))
            def query(variant):
                base = spark.read.parquet(str(path))
                if variant == 'native':
                    valid = (F.col('id') % 100 != 0) & (F.length('payload') > 0)
                else:
                    valid = F.udf(lambda i,p: i % 100 != 0 and len(p)>0, T.BooleanType())('id','payload')
                return base.filter(valid).agg(F.count('*').alias('accepted'), F.sum('id').alias('id_sum'))
            bad = (n-1)//100 + 1
            expected = {'accepted':n-bad, 'id_sum':n*(n-1)//2 - 100*bad*(bad-1)//2 if n>1 else None}
            case = {'rows':n, 'input_identity':digest([1,n]), 'parquet_bytes':sum(p.stat().st_size for p in path.glob('*.parquet')),
                    'files':len(list(path.glob('*.parquet'))), 'expected':expected, 'variants':{}}
            names = ['native','python_udf']; timings = {name:[] for name in names}; plans = {}
            for name in names:
                spark.sparkContext.setJobGroup(f'warm-{n}-{name}','Warm-up')
                assert query(name).first().asDict() == expected
            for rep in range(repetitions):
                for name in names[rep%2:] + names[:rep%2]:
                    spark.catalog.clearCache(); group=f'measure-{n}-{name}-{rep}'
                    spark.sparkContext.setJobGroup(group,'Validation benchmark')
                    q=query(name); before=time.perf_counter(); got=q.first().asDict(); elapsed=time.perf_counter()-before
                    assert got==expected, (got,expected)
                    plans[name]=q._jdf.queryExecution().executedPlan().toString()
                    timings[name].append({'seconds':elapsed,'job_group':group})
            for name in names:
                values=[v['seconds'] for v in timings[name]]
                case['variants'][name]={'trials':timings[name], 'median_seconds':statistics.median(values),
                                       'min_seconds':min(values),'max_seconds':max(values),'plan':plans[name]}
            case['equivalent_results']=True; report['cases'].append(case)
    finally:
        spark.stop()
        os.environ.pop('OLIST_SPARK_EVENT_DIR',None)
    report['task_metrics_by_job_group']=event_metrics(events)
    report['elapsed_seconds_including_setup']=time.perf_counter()-started
    report['status']='passed'
    output=Path('evidence/portfolio-benchmark.json')
    output.write_text(json.dumps(report,indent=2),encoding='utf-8')
    return report
