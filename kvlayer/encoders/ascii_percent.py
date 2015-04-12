'''Key tuple encoder using ASCII text with percent escaping.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: AsciiPercentEncoder

'''
from __future__ import absolute_import
import uuid

from kvlayer.encoders.base import Encoder
from kvlayer._exceptions import SerializationError


class AsciiPercentEncoder(Encoder):
    '''Key tuple encoder using ASCII text with percent escaping.

    This serializes :class:`~uuid.UUID`, :class:`int`, and :class:`long`
    to 8-byte ASCII hex strings.  :class:`str` escapes ``%`` to ``%25``
    and null bytes to ``%00``.  Then the tuple of byte strings from
    these encodings is joined with null bytes between them.

    This encoding produces readable keys in most cases, but is
    fairly inefficient.  Sort order is preserved except for strings
    containing null and percent bytes, and negative integers.

    .. versionchanged:: 0.5
       This was the default key encoder before this version.

    '''
    config_name = 'ascii_percent'

    DELIMITER = '\0'

    def serialize(self, key, spec):
        '''Convert a key tuple to a database-native byte string.

        `key` must be a tuple at most as long as `spec`.  `spec` is a
        tuple of types, and each of the items in `key` must be an
        instance of the corresponding type in `spec`.  If this is
        not the case, :exc:`exceptions.TypeError` will be raised.

        :param tuple key: key tuple to serialize
        :param tuple spec: key type tuple
        :return: serialized byte string
        :raise exceptions.TypeError: if a key part has the wrong type, or
          `key` is too long
        :raise kvlayer._exceptions.SerializationError: if a key part has
          a type that can't be serialized

        '''
        if spec is None:
            spec = (None,) * len(key)
        if len(key) > len(spec):
            raise TypeError('key too long, wanted {0} parts, got {1}'
                            .format(len(spec), len(key)))
        return self.DELIMITER.join(self._serialize_one(frag, typ)
                                   for (frag, typ) in zip(key, spec))

    def _serialize_one(self, frag, typ):
        '''Serialize one fragment of a key tuple.

        If `typ` is :const:`None`, then any type of `frag` is
        acceptable.

        :param frag: part of the key
        :param type typ: expected type of `frag`
        :return: serialized byte string
        :raise exceptions.TypeError: if `frag` has the wrong type
        :raise kvlayer._exceptions.SerializationError: if `typ` isn't
          something this can serialize

        '''
        if typ is not None and not isinstance(frag, typ):
            raise TypeError('expected {0} in key but got {1} ({2!r})'
                            .format(typ, type(frag), frag))
        if isinstance(frag, uuid.UUID) or hasattr(frag, 'hex'):
            return frag.hex
        elif isinstance(frag, (int, long)):
            # format an int to the same number of nybbles as a UUID
            return '{0:032x}'.format(frag)
        elif isinstance(frag, basestring):
            return str(frag.replace('%', '%25').replace('\x00', '%00'))
        elif typ is None:
            raise TypeError('could not serialize {0} key fragment ({1!r})'
                            .format(type(frag), frag))
        else:
            raise SerializationError('could not serialize {0} key fragment'
                                     .format(typ))

    def make_start_key(self, key, spec):
        '''Convert a partial key to the start of a scan range.

        This returns a byte string such that all key tuples greater
        than or equal to `key` :meth:`serialize` to greater than or
        equal to the returned byte string.  If `key` is shorter than
        `spec` then it recognizes all longer key tuples with the same
        prefix.  This generally works like :meth:`serialize`.

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
        dbkey = self.serialize(key, spec)
        if spec and len(key) > 0 and len(key) < len(spec):
            dbkey += self.DELIMITER
        return dbkey

    def make_end_key(self, key, spec):
        '''Convert a partial key to the end of a scan range.

        This returns a byte string such that all key tuples less than
        or equal to `key` :meth:`serialize` to less than or equal to
        the returned byte string.  If `key` is shorter than `spec`
        then it recognizes all longer key tuples with the same prefix.
        This generally works like :meth:`serialize`.

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
        dbkey = self.serialize(key, spec)
        if spec:
            if len(key) == 0:
                dbkey += '\xff'
            elif len(key) < len(spec):
                dbkey += chr(ord(self.DELIMITER) + 1)
            else:
                dbkey += '\x00'
        else:
            dbkey += '\xff'
        return dbkey

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
        parts = dbkey.split(self.DELIMITER)
        if len(parts) != len(spec):
            raise SerializationError(
                'tried to split key into {0} parts but got {1}, '
                'from {2!r}').format(len(key_spec), len(parts), key_str)
        return tuple(self._deserialize_one(frag, typ)
                     for frag, typ in zip(parts, spec))
        
    def _deserialize_one(self, frag, typ):
        '''Deserialize one fragment of a serialized string.

        :param frag: part of the serialized key
        :param type typ: expected type of the result
        :return: instance of `typ`
        :raise exceptions.ValueError: if `frag` doesn't actually
          deserialize to `typ`
        :raise kvlayer._exceptions.SerializationError: if `typ` isn't
          something this can deserialize
        '''
        if typ is int:
            return int(frag, 16)
        elif typ is long:
            return long(frag, 16)
        elif typ == (int, long):
            return long(frag, 16)
        elif typ is uuid.UUID:
            return uuid.UUID(hex=frag)
        elif typ is str:
            return frag.replace('%00', '\x00').replace('%25', '%')
        else:
            raise SerializationError('could not deserialize {0} key fragment'
                                     .format(typ))
