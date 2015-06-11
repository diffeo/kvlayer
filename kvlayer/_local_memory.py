'''Pure in-memory backend for kvlayer.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

The :class:`LocalStorage` backend is mostly useful in test environments:
it stores all :mod:`kvlayer` data in a Python dictionary.

.. autoclass:: AbstractLocalStorage
   :show-inheritance:

.. autoclass:: LocalStorage
   :show-inheritance:

'''
from __future__ import absolute_import
import logging
import time

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection

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
        self._joined_key_cache = dict()

    def setup_namespace(self, table_names, value_types={}):
        super(AbstractLocalStorage, self).setup_namespace(
            table_names, value_types)

        if self._app_name not in self._data:
            self._data[self._app_name] = {}
        if self._namespace not in self._data[self._app_name]:
            self._data[self._app_name][self._namespace] = {}

        for table in table_names:
            if table not in self.data:
                self.data[table] = dict()

        self._connected = True

    def delete_namespace(self):
        if ((self._app_name in self._data and
             self._namespace in self._data[self._app_name])):
            del self._data[self._app_name][self._namespace]
            if not self._data[self._app_name]:
                # empty now? del that too.
                del self._data[self._app_name]

    @property
    def data(self):
        '''The underlying dictionary for this namespace.'''
        return self._data[self._app_name][self._namespace]

    @_requires_connection
    def clear_table(self, table_name):
        self.data[table_name] = dict()

    @_requires_connection
    def put(self, table_name, *keys_and_values, **kwargs):
        start_time = time.time()
        keys_size = 0
        values_size = 0
        num_keys = 0
        for key, val in keys_and_values:
            key_spec = self._table_names[table_name]
            self.check_put_key_value(key, val, table_name, key_spec)
            self.data[table_name][key] = val
            if self._log_stats is not None:
                num_keys += 1
                keys_size += len(self._encoder.serialize(key, key_spec))
                values_size += len(str(val))

        end_time = time.time()
        num_values = num_keys

        self.log_put(table_name, start_time, end_time, num_keys, keys_size,
                     num_values, values_size)

    def _get_joined_key(self, key, key_spec):
        '''To ensure that this acts like big disk-bound DBs, we ensure that
        the sort order coming out of this in-memory mock matches the
        serialized sorted key order.  This requires serializing the
        key according to the key spec.  If an application does this
        frequently, then it can be expensive.  This caches the
        serialized key strings for faster access.

        '''
        _key = (key, key_spec)
        if _key in self._joined_key_cache:
            return self._joined_key_cache[_key]
        else:
            joined_key = self._encoder.serialize(key, key_spec)
            self._joined_key_cache[_key] = joined_key
            return joined_key

    @_requires_connection
    def scan(self, table_name, *key_ranges, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        num_values = 0
        values_size = 0

        key_spec = self._table_names[table_name]
        key_ranges = list(key_ranges)
        if not key_ranges:
            key_ranges = [[None, None]]
        for start, finish in key_ranges:
            total_count = 0
            start = self._encoder.make_start_key(start, key_spec)
            finish = self._encoder.make_end_key(finish, key_spec)
            for key in sorted(self.data[table_name].iterkeys()):
                # given a range, mimic the behavior of DBs that tell
                # you if they failed to find a key
                # LocalStorage does get/put on the Python tuple as the key,
                # stringify for sort comparison
                joined_key = self._get_joined_key(key, key_spec)
                if (start is not None) and (start > joined_key):
                    continue
                if (finish is not None) and (finish < joined_key):
                    continue
                total_count += 1
                val = self.data[table_name][key]
                yield key, val

                if self._log_stats is not None:
                    num_keys += 1
                    keys_size += len(joined_key)
                    num_values += 1
                    values_size += len(str(val))

        end_time = time.time()
        num_values = num_keys
        self.log_scan(table_name, start_time, end_time, num_keys, keys_size,
                      num_values, values_size)

    @_requires_connection
    def get(self, table_name, *keys, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        num_values = 0
        values_size = 0

        for key in keys:
            if self._log_stats is not None:
                key_spec = self._table_names[table_name]
                num_keys += 1
                keys_size += len(self._encoder.serialize(key, key_spec))
            try:
                key, value = key, self.data[table_name][key]
                yield key, value
                num_values += 1
                values_size += len(str(value))
            except KeyError:
                yield key, None

        end_time = time.time()
        self.log_get(table_name, start_time, end_time, num_keys, keys_size,
                     num_values, values_size)

    @_requires_connection
    def delete(self, table_name, *keys):
        start_time = time.time()
        num_keys = 0
        keys_size = 0

        for key in keys:
            if self._log_stats is not None:
                key_spec = self._table_names[table_name]
                num_keys += 1
                keys_size += len(self._encoder.serialize(key, key_spec))
            self.data[table_name].pop(key, None)

        end_time = time.time()
        self.log_delete(table_name, start_time, end_time, num_keys, keys_size)

    def close(self):
        # prevent reading until connected again
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
