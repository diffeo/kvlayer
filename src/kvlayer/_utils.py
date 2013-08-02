'''
Tools for storage implementations.

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import uuid
import itertools
from operator import attrgetter
from bigtree._exceptions import StorageClosed

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
    return map(lambda s: uuid.UUID(hex=''.join(s)), grouper(uuid_str, 32))

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
    if not uuids or uuids[0] == '':
        uuid_str = b''
    else:
        uuid_str = ''.join(map(attrgetter('hex'), uuids))
    uuid_str += padding * ((num_uuids * 32) - len(uuid_str))
    return uuid_str

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
