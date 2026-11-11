import csv
import hashlib
import json
from copy import deepcopy
import pytest
from olist.io_utils import csv_parts, hash_file
from olist.quality import evaluate
from olist.telemetry import operation
from olist.contracts import HEADERS

def test_chunked_csv_preserves_records_quotes_unicode_and_newlines(tmp_path):
    rows = [{'id':str(i), 'text':'Đơn "hàng"\nreview, second line'} for i in range(7)]
    parts = list(csv_parts(iter(rows), ['id','text'], tmp_path, max_rows=3))
    assert [count for _,count in parts] == [3,3,1]
    got=[]
    for path,_ in parts:
        with path.open(encoding='utf-8',newline='') as stream:
            got.extend(csv.DictReader(stream))
        assert hash_file(path, 5) == hashlib.sha256(path.read_bytes()).hexdigest()
    assert got==rows

def test_empty_and_exact_boundary_parts(tmp_path):
    empty=list(csv_parts([],['id'],tmp_path/'empty',max_rows=2))
    assert len(empty)==1 and empty[0][1]==0
    exact=list(csv_parts([{'id':'a'},{'id':'b'}],['id'],tmp_path/'full',max_rows=2))
    assert len(exact)==1 and exact[0][1]==2

def test_byte_target_and_nonmaterialized_iterator(tmp_path):
    seen=[]
    def source():
        for i in range(50):
            seen.append(i);yield {'text':'x'*100}
    iterator=csv_parts(source(),['text'],tmp_path,max_rows=50,max_bytes=150)
    path,count=next(iterator)
    assert count==2 and len(seen)==2 and path.is_file()
    iterator.close()

@pytest.mark.parametrize('limit',[0,-1,True,1.5])
def test_invalid_chunk_limit(limit,tmp_path):
    with pytest.raises(ValueError):list(csv_parts([],['id'],tmp_path,max_rows=limit))

def valid_manifest():
    return {'dataset_version':'test','accounting':{table:{'raw':100,'accepted':99,'rejected':1,'duplicate':0} for table in HEADERS}}

def test_quality_gate_success_and_independent_failures():
    original=valid_manifest()
    assert evaluate(original)['status']=='passed'
    for key,value,rule in [('accepted',98,'accounting_mismatch'),('raw',True,'missing_or_invalid_counts'),('raw',-1,'missing_or_invalid_counts')]:
        altered=deepcopy(original);altered['accounting']['orders'][key]=value
        assert rule in {x['rule'] for x in evaluate(altered)['failures']}
    altered=deepcopy(original);del altered['accounting']['orders']
    assert evaluate(altered)['status']=='failed'
    assert evaluate(original,max_reject_rate=0)['status']=='failed'

def test_operation_records_success_and_sanitized_failure(tmp_path):
    with operation('ok',tmp_path):pass
    with pytest.raises(RuntimeError):
        with operation('failure',tmp_path):raise RuntimeError('do-not-record-this-secret')
    records=[json.loads(p.read_text()) for p in tmp_path.glob('*.json')]
    assert {r['status'] for r in records}=={'succeeded','failed'}
    assert all(r['elapsed_seconds']>=0 for r in records)
    assert not any('do-not-record-this-secret' in p.read_text() for p in tmp_path.glob('*'))

def test_successful_system_exit_is_not_a_failed_operation(tmp_path):
    with pytest.raises(SystemExit):
        with operation('pytest',tmp_path): raise SystemExit(0)
    assert json.loads(next(tmp_path.glob('*.json')).read_text())['status']=='succeeded'

def test_spark_task_metrics_keep_groups_spill_and_failures(tmp_path):
    from olist.portfolio import event_metrics
    events=[{'Event':'SparkListenerJobStart','Stage IDs':[3],'Properties':{'spark.jobGroup.id':'trial'}},
            {'Event':'SparkListenerTaskEnd','Stage ID':3,'Task End Reason':{'Reason':'Success'},
             'Task Metrics':{'Executor Run Time':50,'Disk Bytes Spilled':20,'Peak Execution Memory':128,
                             'Shuffle Read Metrics':{'Remote Bytes Read':3,'Local Bytes Read':4},
                             'Shuffle Write Metrics':{'Shuffle Bytes Written':9}}},
            {'Event':'SparkListenerTaskEnd','Stage ID':3,'Task End Reason':{'Reason':'ExceptionFailure'},'Task Metrics':{}}]
    (tmp_path/'events').write_text('\n'.join(json.dumps(e) for e in events))
    m=event_metrics(tmp_path)['trial']
    assert m['tasks']==2 and m['failed_tasks']==1 and m['disk_spill_bytes']==20
    assert m['shuffle_read_bytes']==7 and m['shuffle_write_bytes']==9 and m['max_task_execution_memory_bytes']==128

def test_http_body_reader_preserves_csv_at_eof():
    from io import BytesIO,BufferedReader,TextIOWrapper
    from olist.io_utils import BodyReader
    from botocore.response import StreamingBody
    body=StreamingBody(BytesIO(b'id,text\r\n1,"hello\nworld"\r\n'),None)
    with TextIOWrapper(BufferedReader(BodyReader(body)),encoding='utf-8',newline='') as text:
        assert list(csv.DictReader(text))==[{'id':'1','text':'hello\nworld'}]
    assert body._raw_stream.closed
