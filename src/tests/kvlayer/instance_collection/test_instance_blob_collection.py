import os
import sys
import yaml
import time
import pytest
from kvlayer.instance_collection import InstanceCollection
from kvlayer.instance_collection._import import import_hook

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

def load(blob):
    return Thing(blob)


def test_import():
    m = import_hook('tests.kvlayer.instance_collection.test_instance_blob_collection',
                    fromlist=['foo']
    )
    thing = m.load('hi: bye')


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


