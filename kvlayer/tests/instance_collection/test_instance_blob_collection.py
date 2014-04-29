from __future__ import absolute_import
from cStringIO import StringIO
import gzip
import json
import os
import sys
import tempfile
import time

import pytest
import yaml

from kvlayer.instance_collection import InstanceCollection, BlobCollection, register, Chunk
from kvlayer._exceptions import SerializationError
from streamcorpus._chunk import serialize

class Thing(object):
    def __init__(self, blob=None):
        self.data = dict()
        if blob is not None:
            self.loads(blob)

    def dumps(self):
        return yaml.dump(self.data)

    def loads(self, blob):
        self.data = yaml.load(blob)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def do_more_things(self):
        self.data['doing'] = 'something'

    def __eq__(self, other):
        return self.data == other.data

class ThingSerializer(object):

    def __init__(self):
        self.config = {}

    def loads(self, blob):
        if self.config.get('compress') == 'gz':
            fh = StringIO(blob)
            gz = gzip.GzipFile(fileobj=fh, mode='rb')
            blob = gz.read()
        return Thing(blob)

    def dumps(self, thing):
        blob = thing.dumps()        
        if self.config.get('compress') == 'gz':
            fh = StringIO()
            gz = gzip.GzipFile(fileobj=fh, mode='wb')
            gz.write(blob)
            gz.flush()
            gz.close()
            blob = fh.getvalue()
        return blob

    def configure(self, config):
        self.config = config

class ThingCollection(InstanceCollection):
    def __missing__(self, key):
        self.insert(key, Thing(), 'Thing')

class OtherThingCollection(InstanceCollection):
    def __missing__(self, key):
        self.insert(key, Thing(), 'Thing')

def test_instance_collection():

    register('Thing', ThingSerializer)

    ic = ThingCollection()
    ic.insert('thing1', Thing(yaml.dump(dict(hello='people'))), 'Thing')
    ic['thing1']['another'] = 'more'
    ic['thing1'].do_more_things()
    ic_str = ic.dumps()
    
    ic2 = ThingCollection(ic_str)

    ## check laziness
    assert 'thing1' not in ic2._instances

    assert ic2['thing1']['another'] == 'more'
    assert 'thing1' in ic2._instances

    assert ic2['thing1']['hello'] == 'people'
    assert ic2['thing1']['doing'] == 'something'

def test_instance_collection_gzip():

    register('Thing', ThingSerializer)

    ic = ThingCollection()
    ic.insert('thing1', Thing(yaml.dump(dict(hello='people'))), 'Thing', config=dict(compress='gz'))
    ic['thing1']['another'] = 'more'
    ic['thing1'].do_more_things()
    ic_str = ic.dumps()
    
    ic2 = ThingCollection(ic_str)
    
    fh = StringIO(ic2._bc.typed_blobs['thing1'].blob)
    gz = gzip.GzipFile(fileobj=fh, mode='rb')
    blob = gz.read()
    tb_data = yaml.load(blob)
    assert 'hello' in tb_data
    assert isinstance(tb_data, dict)

    ## check laziness
    assert 'thing1' not in ic2._instances

    assert ic2['thing1']['another'] == 'more'
    assert 'thing1' in ic2._instances

    assert ic2['thing1']['hello'] == 'people'
    assert ic2['thing1']['doing'] == 'something'


def test_instance_collection_yaml_json():

    ic = ThingCollection()
    ic.insert('thing2', dict(hello='people'), 'yaml')
    ic['thing2']['another'] = 'more'
    ic.insert('thing3', dict(hello='people2'), 'json')
    ic_str = ic.dumps()
    
    ic2 = ThingCollection(ic_str)

    ## check laziness
    assert 'thing2' not in ic2._instances

    assert ic2['thing2']['another'] == 'more'
    assert 'thing2' in ic2._instances

    assert ic2['thing2']['hello'] == 'people'
    assert ic2['thing3']['hello'] == 'people2'


## TODO: explain how this number is picked
EXPECTED_INSTANCE_COLLECTION_SERIALIZATION_SPEED = 50

@pytest.mark.performance
def test_throughput_instance_collection():

    register('Thing', ThingSerializer)
    ic = ThingCollection()
    ic.insert('thing1', Thing(yaml.dump(dict(one_mb=' ' * 2**20))), 'Thing')
    ic_str = ic.dumps()
    
    start_time = time.time()
    num = 100
    for i in range(num):
        ic2 = ThingCollection(ic_str)
        ic2.dumps()
    elapsed = time.time() - start_time
    rate = float(num) / elapsed
    print '%d MB in %.1f sec --> %.1f MB per sec' % (num, elapsed, rate)
    assert rate > EXPECTED_INSTANCE_COLLECTION_SERIALIZATION_SPEED


def test_chunk_blob_collection():
    tmp = tempfile.NamedTemporaryFile(mode='wb')
    o_chunk = Chunk(file_obj=tmp, mode='wb')

    register('Thing', ThingSerializer)
    ic = ThingCollection()
    ic.insert('thing1', Thing(yaml.dump(dict(hello='people'))), 'Thing')
    ic['thing1']['another'] = 'more'

    o_chunk.add(ic)
    o_chunk.flush()

    ic2 = list(Chunk(tmp.name, mode='rb'))[0]
    assert ic2['thing1'].data

    assert ic['thing1'] == ic2['thing1'], (ic['thing1'].data, ic2['thing1'].data)

def test_cant_create_instancecollection():
    '''InstanceCollection(str) must have its runtime type exactly match the
    serialized type in str'''
    ic = ThingCollection()
    ic.insert('thing', dict(hello='people'), 'yaml')
    ic_str = ic.dumps()

    with pytest.raises(SerializationError):
        ic2 = OtherThingCollection(ic_str)

def test_strange_collection_type():
    bc = BlobCollection()
    bc.collection_type = __name__ + ".StrangeCollection"
    bc_str = serialize(bc)

    with pytest.raises(SerializationError):
        for ic in Chunk(data=bc_str):
            pass
    
