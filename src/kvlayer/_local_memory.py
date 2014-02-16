'''
Implementation of AbstractStorage using local memory
instead of a DB backend

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import abc
import logging
from kvlayer._exceptions import MissingID
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection, join_uuids

logger = logging.getLogger('kvlayer')

class StorageSingleton(type):

    __metaclass__ = abc.ABCMeta

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(StorageSingleton, cls).__call__(*args, **kwargs)
        cls._instances[cls]._connected = True
        return cls._instances[cls]


class ABCMeta_StorageSingleton(abc.ABCMeta, StorageSingleton):
    '''
    A class can only have one meta class.  Since AbstractStorage
    has a metaclass of abc.ABCMeta, and we want local storage to
    be a signleton, we create a metaclass here which combines
    both and will be used by LocalStorage and all classes that
    inherit from it (e.g. FileStorage.)
    '''
    pass


class LocalStorage(AbstractStorage):
    '''
    local in-memory storage for testing
    '''

    __metaclass__ = ABCMeta_StorageSingleton

    def __init__(self, config):
        ## singleton prevents use of super
        #super(LocalStorage, self).__init__(config)
        AbstractStorage.__init__(self, config)
        self._data = {}

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        logger.debug('creating tables: %r', table_names)
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
            ex = self.check_put_key_value(key, val, table_name, self._table_names[table_name])
            if ex:
                raise ex
            self._data[table_name][key] = val
            count += 1

    @_requires_connection
    def scan(self, table_name, *key_ranges, **kwargs):
        num_uuids = self._table_names[table_name]
        key_ranges = list(key_ranges)
        specific_key_range = True
        if not key_ranges:
            key_ranges = [[('',), ('',)]]
            specific_key_range = False
        for start, finish in key_ranges:
            total_count = 0
            start = len(start)>0  and join_uuids(*start,  num_uuids=num_uuids, padding='0') or '0' * 32 * num_uuids
            finish = len(finish)>0 and join_uuids(*finish, num_uuids=num_uuids, padding='f') or 'f' * 32 * num_uuids
            for key in sorted(self._data[table_name].iterkeys()):
                ## given a range, mimic the behavior of DBs that tell
                ## you if they failed to find a key
                joined_key = join_uuids(*key)
                if start <= joined_key <= finish:
                    total_count += 1
                    yield key, self._data[table_name][key]
            else:
                if specific_key_range and total_count == 0:
                    ## specified a key range, but found none
                    raise MissingID()

    @_requires_connection
    def get(self, table_name, *keys, **kwargs):
        for key in keys:
            try:
                key, value = key, self._data[table_name][key]
            except KeyError:
                raise MissingID('table_name=%r key: %r' % ( table_name, key))
            yield key, value


    @_requires_connection
    def delete(self, table_name, *keys):
        for key in keys:
            self._data[table_name].pop(key, None)

    def close(self):
        ## prevent reading until connected again
        self._connected = False
