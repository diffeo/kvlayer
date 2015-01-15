'''Key tuple encoder using ASCII text with percent escaping.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: AsciiPercentEncoder

'''
from __future__ import absolute_import
import struct
import uuid

from kvlayer.encoders.base import Encoder
from kvlayer._exceptions import BadKey, SerializationError


class PackedEncoder(Encoder):
    '''Key tuple encoder using compact binary representation.

    This serializes :class:`~uuid.UUID`, :class:`int`, and :class:`long`
    to 8-byte ASCII hex strings.  :class:`str` escapes ``%`` to ``%25``
    and null bytes to ``%00``.  Then the tuple of byte strings from
    these encodings is joined with null bytes between them.

    This encoding produces unreadable binary small keys.  Sort order
    is preserved except for strings containing null and percent bytes,
    and negative integers.

    '''
    config_name = 'packed'

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
            raise BadKey('key too long, wanted {} parts, got {}'
                            .format(len(spec), len(key)))
        return b'\0'.join(self._gen_ser_parts(key, spec))

    def _gen_ser_parts(self, key, spec):
        for i in xrange(len(key)):
            frag = key[i]
            typ = spec[i]
            yield self._serialize_one(frag, typ, i)

    def _serialize_one(self, frag, typ, i):
        '''Serialize one fragment of a key tuple.

        If `typ` is :const:`None`, then any type of `frag` is
        acceptable.

        :param frag: part of the key
        :param type typ: expected type of `frag`
        :param int i: position in key spec list
        :return: serialized byte string
        :raise exceptions.TypeError: if `frag` has the wrong type
        :raise kvlayer._exceptions.SerializationError: if `typ` isn't
          something this can serialize

        '''
        if typ is not None and not isinstance(frag, typ):
            raise BadKey('expected {} in key but got {} ({!r})'
                            .format(typ, type(frag), frag))

        if typ == str:
            # Delimiter is '\0'
            # 'prefix' should sort before 'prefixsuffix'
            # ('\0', '\xff') should sort before ('\0\0','\xff')
            # but '\0\0\0\xff' would sort before '\0\0\xff'
            # SO, we must encode away all '\0' inside strings so that
            # delimiter '\0' always sorts before content.
            frag = frag.replace('\x01', '\x01\x03').replace('\x00', '\x01\x02')
            if i == 0:
                return frag
            else:
                assert len(frag) < 0x0ffff
                return frag + struct.pack('>H', len(frag))
        elif typ == int:
            assert frag <= 0x7fffffff
            return struct.pack('>I', frag + 0x80000000)
        elif (typ == long) or (typ == (int,long)) or (typ == (long,int)):
            assert frag < 0x7fffffffffffffff
            return struct.pack('>Q', frag + 0x8000000000000000)
        elif typ == uuid.UUID:
            return frag.get_bytes()
        else:
            raise BadKey("don't know how to serialize type {}".format(typ))

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
        #if spec and len(key) > 0 and len(key) < len(spec):
        #    dbkey += self.DELIMITER
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
                dbkey += '\x01'
                #dbkey += chr(ord(self.DELIMITER) + 1)
                #dbkey = dbkey[:-1] + chr(ord(dbkey[-1]) + 1)
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
        end = len(dbkey)
        i = len(spec) - 1
        parts = [None] * len(spec)
        while i >= 0:
            si = spec[i]
            if si == int:
                parts[i] = struct.unpack('>I', dbkey[end-4:end])[0] - 0x80000000
                end -= 4
            elif (si == long) or (si == (int,long)) or (si == (long,int)):
                parts[i] = struct.unpack('>Q', dbkey[end-8:end])[0] - 0x8000000000000000
                end -= 8
            elif si == uuid.UUID:
                parts[i] = uuid.UUID(bytes=dbkey[end-16:end])
                end -= 16
            elif si == str:
                if i == 0:
                    ts = dbkey[:end]
                else:
                    strlen = struct.unpack('>H', dbkey[end-2:end])[0]
                    end -= 2
                    ts = dbkey[end - strlen:end]
                    end -= strlen
                # TODO: check that there are no lone '\x01' bytes which would be an invalid encoding
                parts[i] = ts.replace('\x01\x02', '\x00').replace('\x01\x03', '\x01')
            else:
                raise BadKey("don't know how to decode key part type {}".format(si))
            end -= 1 # skip delimiter byte
            i -= 1
        return tuple(parts)
