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

class InstanceCollection(collections.MutableMapping):
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
        elif isinstance(data, collections.Mapping):
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
            i_fh = AugmentedStringIO(data)
            i_transport = TTransport.TBufferedTransport(i_fh)
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
        return '{}(keys={!r}'.format(self.__class__.__name__,
                                     self._bc.typed_blobs.keys())

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
            ## save the deserialized instance for repeated use
            serializer_name = self._bc.typed_blobs[key].serializer
            serializer_class = registered_serializers.get(serializer_name)
            if not serializer_class:
                raise SerializationError(
                    '{!r} has serializer={!r}, but that is not registered'
                    .format(key, serializer_name))
            serializer = serializer_class()
            if self._bc.typed_blobs[key].config:
                serializer.configure(self._bc.typed_blobs[key].config)
            self._bc.typed_blobs[key].blob = serializer.dumps(obj)
        return self._bc
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
            i_fh = AugmentedStringIO(blob_collection_blob)
            i_transport = TTransport.TBufferedTransport(i_fh)
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
            serializer_name = self._bc.typed_blobs[key].serializer
            serializer_class = registered_serializers.get(serializer_name)
            if not serializer_class:
                raise SerializationError(
                    '{!r} has serializer={!r}, but that is not registered'
                    .format(key, serializer_name))
            serializer = serializer_class()
            if self._bc.typed_blobs[key].config:
                serializer.configure(self._bc.typed_blobs[key].config)
            ## hold on to the deserialized instance for repeated use
            self._instances[key] = serializer.loads(self._bc.typed_blobs[key].blob)
        return self._instances[key]

    def __setitem__(self, key, value):
        self.__missing__(key)
        self._instances[key] = value

    def __delitem__(self, key):
        del self._instances[key]
        del self._bc.typed_blobs[key]

    def pop(self, key, default=None):
        if key in self:
            value = self.__getitem__(key)
            self._bc.typed_blobs.pop(key)
            self._instances.pop(key)
            return value
        elif default is not None:
            return default
        else:
            raise IndexError('%r is not in %r' % (key, self))

    def insert(self, key, value, serializer_name, config=None):
        '''Insert a value into the collection with an explicit serializer.

        This allows the caller to insert a value with the name of the
        serializer and an optional configuration block.  The
        serializer must be registered via the :func:`register`
        function.  In most cases, this will be called by the
        :meth:`__missing__` function, and ordinary :meth:`__setitem__`
        will correctly populate the item.

        :param key: mapping key
        :param value: mapping value
        :param str serializer_name: registered name of the binary
          serializer for `value`
        :param dict config: configuration associated with the object

        '''
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
