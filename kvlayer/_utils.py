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


# TODO: update documentation. keys aren't always UUIDs.
# TODO: num_uuids/padding options go away, as it was used for what is now accomplished by make_start_key/make_end_key
# New way: keys are either UUIDs, have attr .hex, or are fed to str()
# If all keys are UUIDs, do padding, otherwise not.
# If all keys are UUIDs, join on '', otherwise join on '\0'
def join_uuids(*uuids, **kwargs):
    '''
    constructs a string by concatenating the hex values of the uuids.

    :param num_uuids: specifies number of UUIDs expected in the input,
    and pads the output string with enough characters to make up the
    difference

    :param padding: a single character used to pad the output string
    to match the length of num_uuids * 32.  Defaults to '0'.  The only
    other value that makes sense is 'f'.
    '''
    num_uuids = kwargs.pop('num_uuids', 0)
    padding = kwargs.pop('padding', '0')
    all_uuid = True
    parts = []
    if not uuids or uuids[0] == '':
        uuid_str = b''
    else:
        for part in uuids:
            if isinstance(part, uuid.UUID) or hasattr(part, 'hex'):
                parts.append(part.hex)
            else:
                all_uuid = False
                parts.append(str(part))
        if all_uuid:
            uuid_str = ''.join(parts)
        else:
            uuid_str = '\0'.join(parts)
    if all_uuid:
        uuid_str += padding * ((num_uuids * 32) - len(uuid_str))
    else:
        if len(parts) < num_uuids:
            if padding == '0':
                # We are making a start key for scan over keys between start and finish
                # but we don't actually need to do anything in that case.
                pass
            elif padding == 'f':
                # We are making a finish key for scan over keys between start and finish.
                # What goes after all things prefixed by 'foo\0...' is 'foo\xff'.
                uuid_str = uuid_str + '\xff'
    return uuid_str


def join_key_fragments(key_fragments, splitter='\0'):
    # kinda underwhelming, probably doesn't need to actually be a function as such
    return splitter.join(key_fragments)


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
