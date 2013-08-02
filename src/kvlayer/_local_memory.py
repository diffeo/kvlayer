'''
Implementation of bigtree.storage.AbstractStorage using local memory
instead of a DB backend

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import math
import uuid
import logging
import hashlib
import traceback
from bigtree._tree_id import TreeID
from bigtree._vertexfamily import VertexFamily
from bigtree._exceptions import MissingID
from bigtree.storage._abstract_storage import AbstractStorage
from bigtree.storage._utils import _requires_connection

logger = logging.getLogger('bigtree.storage.local')

## make LocalStorage a singleton, so it looks like a client to db
def _local_storage_singleton(cls):
    instances = {}
    def getinstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        ## make LocalStorage look like other storage clients by having
        ## it not be connected until you re-access it:
        instances[cls]._connected = True
        return instances[cls]
    return getinstance

@_local_storage_singleton
class LocalStorage(AbstractStorage):
    '''
    local in-memory storage for testing
    '''
    def __init__(self, *args, **kwargs):
        self._data = None

    def setup_namespace(self, namespace, table_names):
        ## just store everything in a dict
        if self._data is None:
            self._data = {table: dict() for table in table_names}
        self._connected = True

    @_requires_connection
    def delete_namespace(self, namespace):
        self._data = None

    @_requires_connection
    def clear_table(self, table_name):
        self._data[table_name] = dict()

    @_requires_connection
    def create_if_missing(self, namespace, table_name, num_uuids):
        if table_name not in self._data:
            self._data[table_name] = dict()

    @_requires_connection
    def put(self, table_name, *keys_and_values, **kwargs):
        count = 0
        for key, val in keys_and_values:
            assert isinstance(key, tuple)
            for key_i in key:
                assert isinstance(key_i, uuid.UUID)
            self._data[table_name][key] = val
            count += 1

    @_requires_connection
    def get(self, table_name, *key_ranges, **kwargs):
        key_ranges = list(key_ranges)
        if not key_ranges:
            key_ranges = [[('',), ('',)]]
        for start, finish in key_ranges:
            found_in_range = []
            for key, val in self._data[table_name].iteritems():
                if start == ('',) == finish:
                    ## this is the base case
                    yield key, val
                    continue
                ## given a range, mimic the behavior of DBs that tell
                ## you if they failed to find a key
                within_range = True
                for i in range(len(start)):
                    if not (start[i] <= key[i] <= finish[i]):
                        within_range = False
                        break
                if within_range:
                    found_in_range.append( (key, val) )
            if found_in_range:
                for key, val in found_in_range:
                    yield key, val
            elif start != ('',):
                ## specified a key range, but found none
                raise MissingID()

    @_requires_connection
    def delete(self, table_name, *keys):
        for key in keys:
            deleted = self._data[table_name].pop(key, None)

    def close(self):
        ## prevent reading until connected again
        self._connected = False
