'''
Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''
import imp
import collections
from cStringIO import StringIO
from kvlayer._exceptions import ProgrammerError
from kvlayer.instance_collection.ttypes import BlobCollection
from kvlayer.instance_collection._import import import_hook

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

class InstanceCollection(collections.Mapping):
    '''
    '''
    def __init__(self, blob_collection_blob=None):
        self._instances = dict()
        self._bc = None
        self.load(blob_collection_blob)

    def load(self, blob_collection_blob):
        '''read raw blob of a BlobCollection
        '''
        self._bc = BlobCollection()
        if blob_collection_blob is not None:
            i_fh = AugmentedStringIO(blob_collection_blob)
            i_transport = TTransport.TBufferedTransport(i_fh)
            i_protocol = protocol(i_transport)
            self._bc.read( i_protocol )
        
    def dump(self):
        for key, obj in self._instances.items():
            self._bc.blobs[key] = obj.dump()
        o_fh = StringIO()
        o_transport = TTransport.TBufferedTransport(o_fh)
        o_protocol = protocol(o_transport)
        self._bc.write( o_protocol )
        o_transport.flush()
        return o_fh.getvalue()
        
    def __getitem__(self, key):
        if key not in self._instances:
            if key not in self._bc.loader_names:
                raise KeyError('%r not in bc.loader_names=%r' % 
                               (key, self._bc.loader_names.keys()))
            ## find module that can load the blob
            loader_name = self._bc.loader_names[key]
            _class = import_hook(loader_name, fromlist=['foo'])
            ## save the deserialized instance for repeated use
            self._instances[key] = _class.load(self._bc.blobs.pop(key))
        return self._instances[key]

    def __setitem__(self, key, value):
        self._instances[key] = value
        self._bc.loader_names[key] = value.__module__
        assert hasattr(value, 'dump')

    def __iter__(self):
        for key in self._bc.blobs.keys():
            yield key

    def __len__(self):
        return len(self._bc.blobs)
