'''
Implementation of AbstractStorage using local memory
instead of a DB backend

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''

import abc
import logging
import time

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection, make_start_key, make_end_key, join_key_fragments

logger = logging.getLogger(__name__)

class AbstractLocalStorage(AbstractStorage):
    """Local in-memory storage for testing.

    This is a base class for storage implementations that use a
    dictionary-like object for actually holding data.

    """

    def __init__(self, *args, **kwargs):
        super(AbstractLocalStorage, self).__init__(*args, **kwargs)
        self._connected = False
        self._raise_on_missing = self._config.get('raise_on_missing', True)

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        super(AbstractLocalStorage, self).setup_namespace(table_names)
        ## just store everything in a dict
        for table in table_names:
            if table not in self._data:
                self._data[table] = dict()
        self._connected = True

    @_requires_connection
    def clear_table(self, table_name):
        self._data[table_name] = dict()

    @_requires_connection
    def put(self, table_name, *keys_and_values, **kwargs):
        start_time = time.time()
        keys_size = 0
        values_size = 0
        num_keys = 0
        for key, val in keys_and_values:
            self.check_put_key_value(key, val, table_name,
                                     self._table_names[table_name])
            self._data[table_name][key] = val
            if self._log_stats is not None:
                num_keys += 1
                keys_size += len(join_key_fragments(key))
                values_size += len(val)

        end_time = time.time()
        num_values = num_keys

        self.log_put(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

    @_requires_connection
    def scan(self, table_name, *key_ranges, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        values_size = 0

        key_spec = self._table_names[table_name]
        key_ranges = list(key_ranges)
        specific_key_range = True
        if not key_ranges:
            key_ranges = [[None, None]]
            specific_key_range = False
        for start, finish in key_ranges:
            total_count = 0
            start = make_start_key(start, key_spec=key_spec)
            finish = make_end_key(finish, key_spec=key_spec)
            for key in sorted(self._data[table_name].iterkeys()):
                ## given a range, mimic the behavior of DBs that tell
                ## you if they failed to find a key
                ## LocalStorage does get/put on the Python tuple as the key, stringify for sort comparison
                joined_key = join_key_fragments(key, key_spec=key_spec)
                if (start is not None) and (start > joined_key):
                    continue
                if (finish is not None) and (finish < joined_key):
                    continue
                total_count += 1
                val = self._data[table_name][key]
                yield key, val

                if self._log_stats is not None:
                    keys_size += len(join_key_fragments(key))
                    values_size += len(val)
                    num_keys += 1
                    values_size += len(val)

        end_time = time.time()
        num_values = num_keys
        self.log_scan(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

    @_requires_connection
    def get(self, table_name, *keys, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        num_values = 0
        values_size = 0

        for key in keys:
            if self._log_stats is not None:
                num_keys += 1
                keys_size += len(join_key_fragments(key))
            try:
                key, value = key, self._data[table_name][key]
                yield key, value
                num_values += 1
                values_size += len(value)
            except KeyError:
                yield key, None

        end_time = time.time()
        self.log_get(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

    @_requires_connection
    def delete(self, table_name, *keys):
        start_time = time.time()
        num_keys = 0
        keys_size = 0

        for key in keys:
            if self._log_stats is not None:
                num_keys += 1
                keys_size += len(join_key_fragments(key))
            self._data[table_name].pop(key, None)

        end_time = time.time()
        self.log_delete(table_name, start_time, end_time, num_keys, keys_size)

    def close(self):
        ## prevent reading until connected again
        self._connected = False

class LocalStorage(AbstractLocalStorage):
    """Local in-memory storage for testing.

    All instances of LocalStorage share the same underlying data.

    As a special case, this ignores almost all of the configuration
    metadata, and can be constructed with no parameters, not even
    `app_name` or `namespace` (defaults will be provided to make
    the base class happy).

    """

    _data = {}
    
    def __init__(self, config=None, app_name=None, namespace=None,
                 *args, **kwargs):
        if config is None:
            config = {}
        if app_name is None and 'app_name' not in config:
            config['app_name'] = 'kvlayer'
        if namespace is None and 'namespace' not in config:
            config['namespace'] = 'kvlayer'
        super(LocalStorage, self).__init__(config=config, app_name=app_name,
                                           namespace=namespace, *args,
                                           **kwargs)

    def delete_namespace(self):
        self._data.clear()
