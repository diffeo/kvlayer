'''
Tools for storage implementations.

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import uuid
import itertools
from operator import attrgetter
from kvlayer._exceptions import StorageClosed, BadKey, SerializationError

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


# Now more generally, split a key string into components.
# Non-UUID keys are joined on '\0', so split on that if present.
def split_uuids(uuid_str):
    if '\0' in uuid_str:
        return uuid_str.split('\0')
    return map(lambda s: uuid.UUID(hex=''.join(s)), grouper(uuid_str, 32))


def join_uuids(*uuids):
    '''
    constructs a string by concatenating the hex values of the uuids.
    '''
    if not uuids or uuids[0] == '':
        uuid_str = b''
    else:
        uuid_str = ''.join(map(lambda x: x.hex, uuids))
    return uuid_str


_default_deserializer_map = {
    int : lambda x: int(x, 16),
    long : lambda x: long(x, 16),
    (int, long) : lambda x: long(x, 16),
    uuid.UUID : lambda x: uuid.UUID(hex=x),
    str : lambda x: x,
}


def split_key(key_str, key_spec, splitter='\0'):
    parts = key_str.split(splitter)
    if len(parts) != len(key_spec):
        raise SerializationError('tried to split key into {0} parts but got {1}, from {2!r}'.format(len(key_spec), len(parts), key_str))
    oparts = []
    for i in xrange(len(key_spec)):
        part = parts[i]
        kt = key_spec[i]
        des = _default_deserializer_map.get(kt)
        if des is None:
            raise SerializationError("don't know how to deserialize key part type {0}".format(kt))
        oparts.append(des(part))
    return tuple(oparts)


def _default_uuid_formatter(x):
    return x.hex

def _default_int_formatter(x):
    return '{0:032x}'.format(x)

def default_key_serializer(x):
    if isinstance(x, uuid.UUID) or hasattr(x, 'hex'):
        return _default_uuid_formatter(x)
    if isinstance(x, (int, long)):
        # format an int to the same number of nybbles as a UUID
        return _default_int_formatter(x)
    return str(x)


def _check_types(key_fragments, key_spec, splitter):
    if key_spec:
        for i in xrange(len(key_fragments)):
            kf = key_fragments[i]
            ks = key_spec[i]
            if not isinstance(kf, ks):
                raise BadKey('key[%s] is %s but wanted %s' % (i, type(kf), ks))


def _serialize_check_join(key_fragments, serializer, splitter):
    parts = []
    for x in key_fragments:
        sx = serializer(x)
        if splitter in sx:
            raise BadKey('serialized key fragment %r must not contain special string %r. If this happens frequently, pick a better splitter or implement escaping on serialized key fragments.' % (sx, splitter))
        parts.append(sx)
    return splitter.join(parts)


def join_key_fragments(key_fragments, splitter='\0', key_spec=None, key_serializer=None):
    _check_types(key_fragments, key_spec, splitter)
    return _serialize_check_join(key_fragments, key_serializer or default_key_serializer, splitter)


def make_start_key(key_fragments, key_spec=None, splitter='\0', key_serializer=None):
    '''
    create a byte string key which will be the start of a scan range
    '''
    if key_fragments is None:
        return None
    pfx = join_key_fragments(key_fragments, splitter, key_spec, key_serializer)
    if ((key_spec and
         len(key_fragments) > 0 and
         len(key_fragments) < len(key_spec))):
        pfx += splitter
    print 'make_start_key %r %r %r'%(key_fragments, key_spec, pfx)
    return pfx


def make_uuid_start_key(key_fragments, num_uuids=0):
    parts = [x.hex for x in key_fragments]
    while len(parts) < num_uuids:
        parts.append('00000000000000000000000000000000')
    return ''.join(parts)


def make_end_key(key_fragments, key_spec=None, splitter='\0', key_serializer=None):
    '''
    create a byte string key which will be the end of a scan range
    '''
    if key_fragments is None:
        return None
    pfx = join_key_fragments(key_fragments, splitter, key_spec, key_serializer)
    if key_spec:
        if len(key_fragments) == 0:
            pfx += '\xff'
        if len(key_fragments) < len(key_spec):
            pfx += splitter[:-1] + chr(ord(splitter[-1]) + 1)
        else:
            pfx += '\0'
    else:
        pfx += '\xff'
    print 'make_end_key %r %r %r'%(key_fragments, key_spec, pfx)
    return pfx


def make_uuid_end_key(key_fragments, num_uuids=0):
    parts = [x.hex for x in key_fragments]
    while len(parts) < num_uuids:
        parts.append('ffffffffffffffffffffffffffffffff')
    return ''.join(parts)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

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
