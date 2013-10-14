'''
Implementation of AbstractStorage using local memory
instead of a DB backend

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import math
import uuid
import logging
import hashlib
import traceback
from kvlayer._exceptions import MissingID
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection

logger = logging.getLogger('kvlayer')

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
    def __init__(self, config):
        self._data = {}
        self._table_names = {}
        self._namespace = config.get('namespace', None)

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        logger.debug('creating tables: %r' % table_names)
        self._table_names.update(table_names)
        ## just store everything in a dict
        for table in table_names:
            if table not in self._data:
                self._data[table] = dict()
        self._connected = True

    @_requires_connection
    def delete_namespace(self):
        self._data = {}

    @_requires_connection
    def clear_table(self, table_name):
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
