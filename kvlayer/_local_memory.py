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
from kvlayer._utils import _requires_connection, make_start_key, make_end_key, join_key_fragments

logger = logging.getLogger(__name__)

class AbstractLocalStorage(AbstractStorage):
    """Local in-memory storage for testing.

    This is a base class for storage implementations that use a
    dictionary-like object for actually holding data.

    """

    def __init__(self):
        super(AbstractLocalStorage, self).__init__()
        self._connected = False
        self._raise_on_missing = self._config.get('raise_on_missing', True)

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        logger.debug('creating tables: %r', table_names)
        self._table_names.update(table_names)
        self.normalize_namespaces(self._table_names)
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
        count = 0
        for key, val in keys_and_values:
            ex = self.check_put_key_value(key, val, table_name, self._table_names[table_name])
            if ex:
                raise ex
            self._data[table_name][key] = val
            count += 1

    @_requires_connection
    def scan(self, table_name, *key_ranges, **kwargs):
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
                yield key, self._data[table_name][key]
            else:
                if specific_key_range and total_count == 0:
                    ## specified a key range, but found none
                    if self._raise_on_missing:
                        raise MissingID()

    @_requires_connection
    def get(self, table_name, *keys, **kwargs):
        for key in keys:
            try:
                key, value = key, self._data[table_name][key]
                yield key, value
            except KeyError:
                if self._raise_on_missing:
                    raise MissingID('table_name=%r key: %r' % ( table_name, key))


    @_requires_connection
    def delete(self, table_name, *keys):
        for key in keys:
            self._data[table_name].pop(key, None)

    def close(self):
        ## prevent reading until connected again
        self._connected = False

class LocalStorage(AbstractLocalStorage):
    """Local in-memory storage for testing.

    All instances of LocalStorage share the same underlying data.
    """

    _data = {}

    def delete_namespace(self):
        self._data.clear()
