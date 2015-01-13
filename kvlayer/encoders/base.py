'''Base class for tuple-to-string encoders.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.

.. autoclass:: Encoder

'''
from __future__ import absolute_import
import abc


class Encoder(object):
    '''Base class for tuple-to-string encoders.

    .. automethod:: serialize
    .. automethod:: deserialize

    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def serialize(self, key, spec):
        '''Convert a key tuple to a database-native byte string.

        `key` must be a tuple at most as long as `spec`.  `spec` is a
        tuple of types, and each of the items in `key` must be an
        instance of the corresponding type in `spec`.  This invariant
        is not checked, but see
        :meth:`kvlayer._abstract_storage.AbstractStorage.check_put_key_value`.
        If an encoder makes strong assumptions about types matching up
        then it should raise :exc:`TypeError`, but this is not
        guaranteed.

        :param tuple key: key tuple to serialize
        :param tuple spec: key type tuple
        :return: serialized byte string
        :raise exceptions.TypeError: if a key part has the wrong type, or
          `key` is too long
        :raise kvlayer._exceptions.SerializationError: if a key part has
          a type that can't be serialized

        '''
        pass

    def make_start_key(self, key, spec):
        '''Convert a partial key to the start of a scan range.

        This returns a byte string such that all key tuples greater
        than or equal to `key` :meth:`serialize` to greater than or
        equal to the returned byte string.  If `key` is shorter than
        `spec` then it recognizes all longer key tuples with the same
        prefix.  This generally works like :meth:`serialize` but may
        return slightly different values in some cases.

        :param tuple key: key tuple to serialize
        :param tuple spec: key type tuple
        :return: serialized byte string
        :raise exceptions.TypeError: if a key part has the wrong type, or
          `key` is too long
        :raise kvlayer._exceptions.SerializationError: if a key part has
          a type that can't be serialized

        '''
        if key is None:
            return None
        return self.serialize(key, spec)

    def make_end_key(self, key, spec):
        '''Convert a partial key to the end of a scan range.

        This returns a byte string such that all key tuples less than
        or equal to `key` :meth:`serialize` to less than or equal to
        the returned byte string.  If `key` is shorter than `spec`
        then it recognizes all longer key tuples with the same prefix.
        This generally works like :meth:`serialize` but may return
        slightly different values in some cases.

        :param tuple key: key tuple to serialize
        :param tuple spec: key type tuple
        :return: serialized byte string
        :raise exceptions.TypeError: if a key part has the wrong type, or
          `key` is too long
        :raise kvlayer._exceptions.SerializationError: if a key part has
          a type that can't be serialized

        '''
        if key is None:
            return None
        return self.serialize(key, spec)

    @abc.abstractmethod
    def deserialize(self, dbkey, spec):
        '''Convert a database-native byte string to a key tuple.

        `dbkey` is expected to have the same format as a call to
        :meth:`serialize` for a key as long as `spec`.  The return
        value will always have the same length as `spec`.

        :param bytes dbkey: key byte string from database
        :param tuple spec: key type tuple
        :return: deserialized key tuple
        :raise exceptions.ValueError: if `dbkey` is wrong
        :raise kvlayer._exceptions.SerializationError: if `spec`
          contains types that cannot be deserialized

        '''
        pass
