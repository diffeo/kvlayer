'''
Tools for storage implementations.

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import uuid
import itertools
from operator import attrgetter
from kvlayer._exceptions import StorageClosed

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


def default_key_serializer(x):
    if isinstance(x, uuid.UUID) or hasattr(x, 'hex'):
        return x.hex
    return str(x)


def join_key_fragments(key_fragments, splitter='\0', uuid_mode=True, key_serializer=None):
    # kinda underwhelming, probably doesn't need to actually be a function as such
    if uuid_mode:
        return b''.join(map(lambda x: x.hex, key_fragments))
    if key_serializer is None:
        key_serializer = default_key_serializer
    return splitter.join(map(key_serializer, key_fragments))


def make_start_key(key_fragments, uuid_mode=True, num_uuids=0, splitter='\0'):
    '''
    create a byte string key which will be the start of a scan range
    '''
    if key_fragments is None:
        return None
    if uuid_mode:
        return make_uuid_start_key(key_fragments, num_uuids)
    else:
        return splitter.join(key_fragments)


def make_uuid_start_key(key_fragments, num_uuids=0):
    parts = [x.hex for x in key_fragments]
    while len(parts) < num_uuids:
        parts.append('00000000000000000000000000000000')
    return ''.join(parts)


def make_end_key(key_fragments, uuid_mode=True, num_uuids=0, splitter='\0'):
    '''
    create a byte string key which will be the end of a scan range
    '''
    if key_fragments is None:
        return None
    if uuid_mode:
        return make_uuid_end_key(key_fragments, num_uuids)
    else:
        return splitter.join(key_fragments) + '\xff'


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
