import pytest
from olist import replay

class Message:
    def topic(self): return 'olist-lifecycle-v1'
    def partition(self): return 0
    def offset(self): return 17

class Producer:
    def __init__(self): self.calls=[]
    def produce(self,topic,**kwargs):
        self.calls.append(kwargs['value'])
        kwargs['on_delivery'](None,Message())
    def flush(self,timeout): return 0

def test_journal_rejects_changed_input_and_destination(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    fake=Producer()
    monkeypatch.setattr(replay,'producer',lambda:(fake,'olist-lifecycle-v1'))
    monkeypatch.setattr(replay.time,'sleep',lambda _:None)
    payloads=[{'event_id':'a'},'{malformed']
    assert replay.send_payloads(payloads,'run')['sent_this_attempt']==2
    assert replay.send_payloads(payloads,'run')['sent_this_attempt']==0
    for changed in [[{'event_id':'b'},'{malformed'],payloads[:1],payloads+['extra']]:
        with pytest.raises(ValueError,match='changed'): replay.send_payloads(changed,'run')
    monkeypatch.setenv('KAFKA_TOPIC','another-topic')
    with pytest.raises(ValueError,match='changed'): replay.send_payloads(payloads,'run')
    assert len(fake.calls)==2
    assert not list((tmp_path/'data/runtime/replay').glob('*.lock'))

def test_journal_error_releases_lock(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    def unavailable(): raise RuntimeError('unavailable')
    monkeypatch.setattr(replay,'producer',unavailable)
    with pytest.raises(RuntimeError): replay.send_payloads(['x'],'run')
    assert not list((tmp_path/'data/runtime/replay').glob('*.lock'))
