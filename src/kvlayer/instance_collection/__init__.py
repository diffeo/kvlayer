'''
Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''
import collections
from cStringIO import StringIO
from kvlayer._exceptions import ProgrammerError

## this enables 
#streamcorpus.Chunk(..., message=kvlayer.instance_collection.BlobCollection)
from kvlayer.instance_collection.ttypes import BlobCollection, TypedBlob

from thrift.transport import TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolAccelerated
try:
    from thrift.protocol import fastbinary
    ## use faster C program to read/write
    protocol = TBinaryProtocolAccelerated

except Exception, exc:
    sys.exit('failed to load thrift.protocol.fastbinary')

    #fastbinary_import_failure = exc
    ## fall back to pure python
    #protocol = TBinaryProtocol

class AugmentedStringIO(object):
    def __init__(self, blob):
        fh = StringIO(blob)
        self._fh = fh
        self.getvalue = fh.getvalue
        self.seek = fh.seek
        self.close = fh.close
        self.read = fh.read

    def readAll(self, sz):
        '''
        This method allows TBinaryProtocolAccelerated to actually function.

        Copied from here
        http://svn.apache.org/repos/asf/hive/trunk/service/lib/py/thrift/transport/TTransport.py
        '''
        buff = ''
        have = 0
        while (have < sz):
            chunk = self.read(sz - have)
            have += len(chunk)
            buff += chunk

            if len(chunk) == 0:
                raise EOFError()

        return buff

import json
import yaml
yaml.loads = yaml.load
yaml.dumps = yaml.dump

## global singleton dict of registered serializers
registered_serializers = dict(
    ## include "yaml" and "json" as defaults
    # must provide a callable that returns a thing with dumps/loads
    yaml=lambda: yaml,
    json=lambda: json,
    )

def register(name, serializer):
    global registered_serializers
    registered_serializers[name] = serializer

class InstanceCollection(collections.Mapping):
    '''
    '''
    def __init__(self, blob_collection_blob=None):
        self._instances = dict()
        if isinstance(blob_collection_blob, BlobCollection):
            self._bc = blob_collection_blob
        else:
            self._bc = None
            self.loads(blob_collection_blob)

    def loads(self, blob_collection_blob):
        '''read raw blob of a BlobCollection
        '''
        self._bc = BlobCollection()
        if blob_collection_blob is not None:
            i_fh = AugmentedStringIO(blob_collection_blob)
            i_transport = TTransport.TBufferedTransport(i_fh)
            i_protocol = protocol(i_transport)
            self._bc.read( i_protocol )
        
    def dumps(self):
        for key, obj in self._instances.items():
            ## save the deserialized instance for repeated use
            serializer_name = self._bc.typed_blobs[key].serializer
            serializer_class = registered_serializers.get(serializer_name)
            if not serializer_class:
                raise ProgrammerError('%r has serializer=%r, but that is not registered'
                                      % (key, serializer_name))
            serializer = serializer_class()
            if self._bc.typed_blobs[key].config:
                serializer.configure(self._bc.typed_blobs[key].config)
            self._bc.typed_blobs[key].blob = serializer.dumps(obj)
        o_fh = StringIO()
        o_transport = TTransport.TBufferedTransport(o_fh)
        o_protocol = protocol(o_transport)
        self._bc.write( o_protocol )
        o_transport.flush()
        return o_fh.getvalue()
        
    def __getitem__(self, key):
        if key not in self._instances:
            if key not in self._bc.typed_blobs:
                raise KeyError('%r not in bc.typed_blobs=%r' % 
                               (key, self._bc.typed_blobs.keys()))
            ## save the deserialized instance for repeated use
            serializer_name = self._bc.typed_blobs[key].serializer
            serializer_class = registered_serializers.get(serializer_name)
            if not serializer_class:
                raise ProgrammerError('%r has serializer=%r, but that is not registered'
                                      % (key, serializer_name))
            serializer = serializer_class()
            if self._bc.typed_blobs[key].config:
                serializer.configure(self._bc.typed_blobs[key].config)
            self._instances[key] = serializer.loads(self._bc.typed_blobs[key].blob)
        return self._instances[key]

    def __setitem__(self, key, value):
        raise ProgrammerError('use InstanceCollection.insert(key, value, serializer_name) instead of directly setting an item')

    def insert(self, key, value, serializer_name, config=None):
        global registered_serializers
        if serializer_name not in registered_serializers:
            raise ProgrammerError('serializer_name=%r is not registered'
                                  % serializer_name)
        self._instances[key] = value
        if config is None:
            config = {}
        self._bc.typed_blobs[key] = TypedBlob(serializer = serializer_name,
                                        config = config)

    def __iter__(self):
        for key in self._bc.typed_blobs.keys():
            yield key

    def __len__(self):
        return len(self._bc.typed_blobs)


def InstanceCollection_write_wrapper(ic):
    '''cause the BlobCollection to get populated (or refreshed) with
    current serialized data, and return it.
    '''
    s = ic.dumps()
    return ic._bc

## Use the Chunk implementation from streamcorpus, which handles
## reading and writing of new chunk files.
from streamcorpus import Chunk as _Chunk

class Chunk(_Chunk):
    def __init__(self, path=None, data=None, file_obj=None, mode='rb',
                 ## must override the constructor to make our message
                 ## type the default.
                 message=BlobCollection,
                 read_wrapper=InstanceCollection, 
                 write_wrapper=InstanceCollection_write_wrapper,
                 ):
        _Chunk.__init__(
            self, path, data, file_obj, mode, 
            message=message,
            read_wrapper=read_wrapper,
            write_wrapper=write_wrapper,
        )

