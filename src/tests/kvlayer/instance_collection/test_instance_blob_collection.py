import os
import sys
import json
import yaml
import time
import pytest
import tempfile
import streamcorpus
from kvlayer.instance_collection import InstanceCollection, BlobCollection

class Thing(object):
    def __init__(self, blob=None):
        self.data = dict()
        if blob is not None:
            self.load(blob)

    def dump(self):
        return yaml.dump(self.data)

    def load(self, blob):
        self.data = yaml.load(blob)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def do_more_things(self):
        self.data['doing'] = 'something'

    def __getstate__(self):
        '''generate a state string for pickle'''
        return json.dumps(self.data)

    def __setstate__(self, state):
        self.data = json.loads(state)
        self.used_setstate = True


def test_instance_collection():
    ic = InstanceCollection()
    ic['thing1'] = Thing(yaml.dump(dict(hello='people')))
    ic['thing1']['another'] = 'more'
    ic['thing1'].do_more_things()
    ic_str = ic.dump()
    
    ic2 = InstanceCollection(ic_str)

    ## check laziness
    assert 'thing1' in ic2._bc.blobs
    assert 'thing1' not in ic2._instances

    assert ic2['thing1']['another'] == 'more'
    assert 'thing1' not in ic2._bc.blobs
    assert 'thing1' in ic2._instances
    assert ic2['thing1'].used_setstate

    assert ic2['thing1']['hello'] == 'people'
    assert ic2['thing1']['doing'] == 'something'


@pytest.mark.performance
def test_throughput_instance_collection():
    ic = InstanceCollection()
    ic['thing1'] = Thing(yaml.dump(dict(one_mb=' ' * 2**20)))
    ic_str = ic.dump()
    
    start_time = time.time()
    num = 100
    for i in range(num):
        ic2 = InstanceCollection(ic_str)
        ic2.dump()
    elapsed = time.time() - start_time
    rate = float(num) / elapsed
    print '%d MB in %.1f sec --> %.1f MB per sec' % (num, elapsed, rate)
    assert rate > 100

@pytest.mark.xfail ## need to enhance streamcorpus.Chunk to have
                   ## 'wrapper' kwarg
def test_chunk_blob_collection():
    tmp = tempfile.NamedTemporaryFile(mode='wb')
    o_chunk = streamcorpus.Chunk(file_obj=tmp, mode='wb', message=BlobCollection)

    ic = InstanceCollection()
    ic['thing1'] = Thing(yaml.dump(dict(hello='people')))
    ic['thing1']['another'] = 'more'

    o_chunk.add(ic)
    tmp.flush()

    for ic2 in streamcorpus.Chunk(tmp.name, message=BlobCollection):
        pass

    assert ic['thing1'] == ic2['thing1']

    
