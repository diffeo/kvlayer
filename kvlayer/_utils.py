'''
Tools for storage implementations.

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import uuid
import itertools
from kvlayer._exceptions import StorageClosed, BadKey, SerializationError


_DELIMITER = '\0'


_deserializer_map = {
    int: lambda x: int(x, 16),
    long: lambda x: long(x, 16),
    (int, long): lambda x: long(x, 16),
    uuid.UUID: lambda x: uuid.UUID(hex=x),
    str: lambda x: x,
}


def deserialize_key(key_str, key_spec):
    '''
    Deserializes a raw key string from a database backend to a tuple
    of key fragments. Each fragment is initialized with its appropriate
    constructor from ``key_spec``.
 
    If ``len(key_fragments) != len(key_spec)`` or if there is an
    unrecognized type in ``key_spec``, then a ``SerializationError``
    is raised.
    '''
    parts = key_str.split(_DELIMITER)
    if len(parts) != len(key_spec):
        raise SerializationError(
            ('tried to split key into {0} parts but got {1}, '
             'from {2!r}').format(len(key_spec), len(parts), key_str))
    return tuple(_key_frag_deserialize(f, t)
                 for f, t in itertools.izip(parts, key_spec))


def serialize_key(key_fragments, key_spec=None):
    '''
    Serializes a list of ``key_fragments`` to a key suitable for use
    by a ``kvlayer`` backend.

    If ``key_spec`` is present, then it must be an iterable with the
    same size as ``key_fragments`` where each element is a valid
    ``kvlayer`` key constructor (e.g., ``uuid.UUID``, ``int`` or
    ``str``).
    '''
    if key_spec is None:
        key_spec = [None] * len(key_fragments)
    frag_types = itertools.izip(key_fragments, key_spec)
    return _DELIMITER.join(_key_frag_serialize(f, s) for f, s in frag_types)


def make_start_key(key_fragments, key_spec=None):
    '''
    create a byte string key which will be the start of a scan range
    '''
    if key_fragments is None:
        return None
    pfx = serialize_key(key_fragments, key_spec)
    if (key_spec and
        len(key_fragments) > 0 and
        len(key_fragments) < len(key_spec)):
        pfx += _DELIMITER
    return pfx


def make_end_key(key_fragments, key_spec=None):
    '''
    create a byte string key which will be the end of a scan range
    '''
    if key_fragments is None:
        return None
    pfx = serialize_key(key_fragments, key_spec)
    if key_spec:
        if len(key_fragments) == 0:
            pfx += '\xff'
        if len(key_fragments) < len(key_spec):
            pfx += chr(ord(_DELIMITER) + 1)
        else:
            pfx += '\0'
    else:
        pfx += '\xff'
    return pfx


def _key_frag_serialize(frag, spec):
    if spec is not None and not isinstance(frag, spec):
        raise BadKey('key[%s] is %s but wanted %s' % (frag, type(frag), spec))

    if isinstance(frag, uuid.UUID) or hasattr(frag, 'hex'):
        return frag.hex
    elif isinstance(frag, (int, long)):
        # format an int to the same number of nybbles as a UUID
        return '{0:032x}'.format(frag)
    elif isinstance(frag, basestring):
        return str(frag.replace('%', '%25').replace('\x00', '%00'))
    else:
        assert False, 'unreachable'


def _key_frag_deserialize(frag, spec):
    mk = _deserializer_map.get(spec)
    if mk is None:
        raise SerializationError(
            "don't know how to deserialize key part type {0}".format(spec))
    if spec is str:
        return mk(frag.replace('%00', '\x00').replace('%25', '%'))
    else:
        return mk(frag)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


def _requires_connection(func):
    '''
    Decorator for methods on any implementation of AbstractStorage.
    Raises StorageClosed when self._connected is not True.
    '''
    def wrapped_func(self, *args, **kwargs):
        if not self._connected:
            raise StorageClosed()
        return func(self, *args, **kwargs)
    return wrapped_func


def split_uuids(uuid_str):
    '''
    DEPRECATED. Only used in Cassandra backend.
    '''
    if '\0' in uuid_str:
        return uuid_str.split('\0')
    return map(lambda s: uuid.UUID(hex=''.join(s)), grouper(uuid_str, 32))


def join_uuids(*uuids):
    '''
    DEPRECATED. Only used in Cassandra backend.
    '''
    if not uuids or uuids[0] == '':
        uuid_str = b''
    else:
        uuid_str = ''.join(map(lambda x: x.hex, uuids))
    return uuid_str


class batches(object):
    '''
    return lists of length n drawn from an iterable.  The last batch
    may be shorter than n.
    '''
    def __init__(self, iterable, n):
        self.items = iter(iterable)
        self.n = n

    def __iter__(self):
        batch = []
        for i in self.items:
            batch.append(i)
            if len(batch) == self.n:
                yield batch
                batch = []
        if batch:
            yield batch
