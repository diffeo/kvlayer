'''Mapping holding serializable values.

.. Your use of this software is governed by your license agreement.
   Copyright 2012-2014 Diffeo, Inc.

This is used to store dictionaries as values for kvlayer cells.

.. autoclass:: InstanceCollection
   :members:
   :special-members:
   :show-inheritance:

.. autofunction:: register

.. autoclass:: BlobCollection
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: Chunk
   :members:
   :show-inheritance:

'''
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import collections
from cStringIO import StringIO
import importlib
import itertools
import json
import logging
import traceback

import yaml

import streamcorpus
from kvlayer._exceptions import ProgrammerError, SerializationError
from kvlayer.instance_collection.ttypes import BlobCollection, TypedBlob

from thrift.transport import TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, \
    TBinaryProtocolAccelerated
try:
    from thrift.protocol import fastbinary
    protocol = TBinaryProtocolAccelerated
except Exception, exc:
    protocol = TBinaryProtocol


logger = logging.getLogger(__name__)


class YamlSerializer(object):
    def __init__(self, config=None):
        pass

    def loads(self, blob):
        return yaml.load(blob)

    def dumps(self, ob):
        return yaml.dump(ob)

## global singleton dict of registered serializers
registered_serializers = dict(
    ## include "yaml" and "json" as defaults
    # must provide a callable that returns a thing with dumps/loads
    yaml=YamlSerializer,
    json=json,
    )

def register(name, serializer):
    global registered_serializers
    registered_serializers[name] = serializer

# was a MutableMapping, but now it's just an object and it defines all the dict-like operations explicitly.
#class InstanceCollection(collections.MutableMapping):
class InstanceCollection(object):
    '''A mapping of keys to serializable instance types.

    This maintains two sets of data: Python objects probably of some
    consistent type, and serialized versions of the same.  If this is
    created with only binary data, the Python objects will be created
    on demand.

    '''
    __metaclass__ = ABCMeta
    def __init__(self, data=None):
        '''Create a new InstanceCollection.

        `data` can provide data for the object at construction time.
        If missing or :const:`None`, the collection is initially
        empty.  If a :class:`BlobCollection`, the data in the
        collection is used for the initial values of the collection,
        and the keys in this collection are the keys from the blob.
        If a mapping, the collection is populated from the mapping,
        with the :meth:`__missing__` function used to generate
        serializations.  If a string, it contains the serialized
        Thrift version of a :class:`BlobCollection`, which is used as
        described above.

        :param data: initialization data

        '''
        self._instances = dict()
        if isinstance(data, BlobCollection):
            self.blobs = data
        elif isinstance(data, (collections.Mapping, InstanceCollection)):
            self._bc = BlobCollection()
            self._bc.collection_type = self._clsname
            self.update(data)
        else:
            self.loads(data)

    @staticmethod
    def from_data(data):
        '''Create a new InstanceCollection from data.

        `data` should be either a :class:`BlobCollection`, or a
        Thrift- serialized form of one.  The actual type constructed
        will be the :attr:`BlobCollection.collection_type` from the
        provided data.

        :param data: inbound `BlobCollection` or serialization thereof
        :return: new `InstanceCollection` or other serializable type

        '''
        if isinstance(data, BlobCollection):
            bc = data
        else:
            bc = BlobCollection()
            i_transport = TTransport.TMemoryBuffer(data)
            i_protocol = protocol(i_transport)
            bc.read( i_protocol )

        target = bc.collection_type
        parts = target.split('.')
        modname = '.'.join(parts[:-1])
        cls = None
        try:
            mod = importlib.import_module(modname)
        except ValueError, e:
            mod = None
        except ImportError, e:
            mod = None
        if mod is not None:
            cls = getattr(mod, parts[-1], None)
        if cls is None:
            raise SerializationError("can't find collection class {!r}"
                                     .format(target))
        return cls(bc)

    @abstractmethod
    def __missing__(self, key):
        '''Provide a default value for some key.

        This should produce a default value and serialization for
        `key`, and insert it using :meth:`insert`.  Note that this is
        also called from e.g. :meth:`__setitem__()` when inserting a value
        for the first time.

        :param key: key of the mapping

        '''
        raise NotImplemented()

    def __repr__(self):
        return '{}(keys={!r})'.format(self.__class__.__name__,
                                     self.keys())

    @property
    def _clsname(self):
        '''Fully qualified module.Class name of this.'''
        return '{}.{}'.format(self.__class__.__module__,
                              self.__class__.__name__)

    @property
    def blobs(self):
        '''The :class:`BlobCollection` backing this :class:`InstanceCollection`.

        Any instances that have been fetched or added will be serialized
        into the :class:`BlobCollection` before returning it.

        '''
        for key, obj in self._instances.items():
            serializer = self._get_serializer(key)
            self._bc.typed_blobs[key].blob = serializer.dumps(obj)
        return self._bc

    def _get_serializer(self, key):
        if key not in self._bc.typed_blobs:
            ival = self._instances[key]
            assert ival is not None
            serializer_name = type(ival).__name__
            self._bc.typed_blobs[key] = TypedBlob(serializer_name)
        else:
            serializer_name = self._bc.typed_blobs[key].serializer
        serializer_class = registered_serializers.get(serializer_name)
        if not serializer_class:
            raise SerializationError(
                '{!r} has serializer={!r}, but that is not registered'
                .format(key, serializer_name))
        if hasattr(serializer_class, '__call__'):
            serializer = serializer_class(self._bc.typed_blobs[key].config)
        else:
            serializer = serializer_class
        assert hasattr(serializer, 'loads'), 'invalid seralizer {!r} missing method "loads"'.format(serializer)
        assert hasattr(serializer, 'dumps'), 'invalid seralizer {!r} missing method "dumps"'.format(serializer)
        if hasattr(serializer, 'configure') and (key in self._bc.typed_blobs):
            serializer.configure(self._bc.typed_blobs[key].config)
        return serializer

    @blobs.setter
    def blobs(self, bc):
        '''Replace the :class:`BlobCollection` for this :class:`InstanceCollection`.

        All cached instances will be discarded.  The
        :attr:`BlobCollection.collection_type` of the collection must
        be the same as this object's runtime type.

        '''
        if bc.collection_type != self._clsname:
            raise SerializationError(
                'cannot create {} with a collection of {}'
                .format(self._clsname, bc.collection_type))
        self._bc = bc
        self._instances.clear()

    def loads(self, blob_collection_blob):
        '''Replaces the collection using a serialized :class:`BlobCollection`.

        All saved data in this object is discarded.  The provided
        string is parsed as a :class:`BlobCollection` and this collection is
        used as a source of serialized items.  If :const:`None` is provided
        then this collection is emptied.

        :param str blob_collection_blob: if not :const:`None`, then a serialized
          Thrift representation of a :class:`BlobCollection`

        '''
        bc = BlobCollection()
        if blob_collection_blob is None:
            bc.collection_type = self._clsname
        else:
            i_transport = TTransport.TMemoryBuffer(blob_collection_blob)
            i_protocol = protocol(i_transport)
            bc.read( i_protocol )
        self.blobs = bc

    def dumps(self):
        '''Create a serialized :class:`BlobConnection` from this.

        All saved items in this object are serialized, and the
        resulting :class:`BlobCollection` is serialized and returned.
        Passing the resulting string to :meth:`loads` or to the class
        constructor should produce an equivalent object to `self`.

        :return: Thrift-serialized :class:`BlobConnection` string

        '''
        o_fh = StringIO()
        o_transport = TTransport.TBufferedTransport(o_fh)
        o_protocol = protocol(o_transport)
        self.blobs.write( o_protocol )
        o_transport.flush()
        return o_fh.getvalue()

    def __contains__(self, key):
        if key in self._instances:
            return True
        elif key in self._bc.typed_blobs:
            return True
        else:
            return False

    def __getitem__(self, key):
        if key not in self._instances:
            if key not in self._bc.typed_blobs:
                return self.__missing__(key)
            serializer = self._get_serializer(key)
            ## hold on to the deserialized instance for repeated use
            self._instances[key] = serializer.loads(self._bc.typed_blobs[key].blob)
        return self._instances[key]

    def get(self, key, *args):
        if key in self._instances:
            return self._instances[key]
        if key in self._bc.typed_blobs:
            serializer = self._get_serializer(key)
            val = serializer.loads(self._bc.typed_blobs[key].blob)
            self._instances[key] = val
            return val
        if args:
            return args[0]
        return self.__missing__(key)

    def __setitem__(self, key, value):
        self._instances[key] = value

    def __delitem__(self, key):
        ok = False
        if key in self._instances:
            del self._instances[key]
            ok = True
        if key in self._bc.typed_blobs:
            del self._bc.typed_blobs[key]
            ok = True
        if not ok:
            raise KeyError(key)

    def pop(self, key, default=None):
        if key in self:
            value = self.__getitem__(key)
            self._bc.typed_blobs.pop(key, None)
            self._instances.pop(key, None)
            return value
        elif default is not None:
            return default
        else:
            raise IndexError('%r is not in %r' % (key, self))

    def iteritems(self):
        for key in self.iterkeys():
            yield key, self[key]

    def items(self):
        return list(self.iteritems())

    def iterkeys(self):
        for key in self._instances.iterkeys():
            yield key
        for key in self._bc.typed_blobs.keys():
            if key not in self._instances:
                yield key

    def keys(self):
        return list(self.iterkeys())

    def itervalues(self):
        for k,v in self.iteritems():
            yield v

    def values(self):
        return list(self.itervalues())

    def update(self, odict):
        for k,v in odict.iteritems():
            self[k] = v

    __iter__ = iterkeys

    def __len__(self):
        return len(set(iter(self)))


class Chunk(streamcorpus.Chunk):
    '''Chunk implementation that yields :class:`InstanceCollection` objects.

    The default `message` type is :class:`BlobCollection`, and you
    should generally use this.  :meth:`add()` adds, and
    :meth:`__iter__` produces, :class:`InstanceCollection` objects.

    In reality, any type that has a `blobs` property can be passed to
    :meth:`add()`; this property contains a :class:`BlobCollection`
    object.  When iterating, the :class:`BlobCollection` names a type
    in its :attr:`~BlobCollection.collection_type` field, and this
    must be a type that will accept the :class:`BlobCollection` as the
    only positional parameter in its constructor.

    '''
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('message', BlobCollection)
        super(Chunk, self).__init__(*args, **kwargs)
    def add(self, msg):
        super(Chunk, self).add(msg.blobs)
    def __iter__(self):
        return itertools.imap(InstanceCollection.from_data,
                              super(Chunk, self).__iter__())
