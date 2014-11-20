'''
Implementation of AbstractStorage using HBase

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''

import logging
import re
import random
import time

import happybase as hbase

from kvlayer._decorators import retry
from kvlayer._exceptions import ProgrammerError
from kvlayer._abstract_storage import AbstractStorage
from _utils import deserialize_key, make_start_key, make_end_key, serialize_key

logger = logging.getLogger(__name__)


class HBaseStorage(AbstractStorage):
    '''
    HBase storage implements kvlayer's AbstractStorage, which
    manages a set of tables as specified in setup_namespace
    '''
    def __init__(self, *args, **kwargs):
        super(HBaseStorage, self).__init__(*args, **kwargs)

        self._connected = False
        addresses = self._config.get('storage_addresses', [])
        if not addresses:
            raise ProgrammerError('config lacks storage_addresses')

        raddr = random.choice(addresses)
        logger.info('connecting to hbase thrift proxy: %r', raddr)
        self._host, self._port = addr_port(raddr, 9090)
        self._conn = None

        self.max_batch_bytes = int(self._config.get('max_batch_bytes', 10000000))

    config_name = 'hbase'
    default_config = {
        'max_batch_bytes': 10000000,
    }

    @property
    def conn(self):
        # TODO: use hbase.ConnectionPool
        # with self._pool.connection() as connection:
        #   yield connection
        if not self._conn:
            logger.info('connecting to HBase')

            # Real namespaces were supposedly added to HBase 0.96, but
            # HappyBase doesn't seem to support those explicitly. Instead, it
            # namespaces tables with prefixes. One hopes that it uses real
            # namespaces when they're available...
            prefix_sep = '_'
            prefix = '%s%s%s' % (self._app_name, prefix_sep, self._namespace)
            self._conn = hbase.Connection(host=self._host, port=self._port,
                                          table_prefix=prefix,
                                          table_prefix_separator=prefix_sep)
            self._connected = True
        return self._conn

    def _create_table(self, table):
        # See http://goo.gl/X1Tlaf for available options for column families.
        # Explicitly not enabling bloom filters for now because I don't know
        # if they'll make a huge impact. See http://goo.gl/EWprKe for some
        # notes about bloom filters in HBase.
        self.conn.create_table(table, {'d': dict(max_versions=1)})

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace. This operation is idempotent.
        '''
        logger.debug('creating tables: %r', table_names)
        super(HBaseStorage, self).setup_namespace(table_names)
        existing_tables = self.conn.tables()
        for table in table_names:
            if table not in existing_tables:
                self._create_table(table)

    def delete_namespace(self):
        '''
        delete all of the tables within namespace
        '''
        for table in self.conn.tables():
            self._delete_table(table)

    def _delete_table(self, table_name):
        # `disable=True` disables the table and then deletes it.
        # (HBase cannot delete enabled tables.)
        self.conn.delete_table(table_name, disable=True)
        logger.debug('deleted table %s_%s_%s', self._app_name, self._namespace, table_name)

    def clear_table(self, table_name):
        self._delete_table(table_name)
        self._create_table(table_name)

    def put(self, table_name, *keys_and_values, **kwargs):
        start_time = time.time()
        keys_size = 0
        values_size = 0
        num_keys = 0

        key_spec = self._table_names[table_name]
        cur_bytes = 0
        table = self.conn.table(table_name)
        batch = table.batch()
        for key, blob in keys_and_values:
            ex = self.check_put_key_value(key, blob, table_name, key_spec)
            if ex:
                raise ex
            num_keys += 1
            skey = serialize_key(key, key_spec)
            morelen = len(blob) + len(skey)
            keys_size += len(skey)
            values_size += len(blob)
            if (morelen + cur_bytes) >= self.max_batch_bytes:
                logger.debug('len(blob)=%d + cur_bytes=%d >= '
                             'max_batch_bytes = %d',
                             len(blob), cur_bytes,
                             self.max_batch_bytes)
                logger.debug('pre-emptively sending only what has been '
                             'batched, and will send this item in next batch.')
                batch.send()
                batch = table.batch()
                cur_bytes = 0
            batch.put(skey, {'d:d':blob})
            cur_bytes += morelen
        if cur_bytes > 0:
            batch.send()

        end_time = time.time()
        num_values = num_keys

        self.log_put(table_name, start_time, end_time, num_keys, keys_size,
                     num_values, values_size)

    def scan(self, table_name, *key_ranges, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        values_size = 0
        key_spec = self._table_names[table_name]
        if not key_ranges:
            key_ranges = [['', '']]
        table = self.conn.table(table_name)
        for start_key, stop_key in key_ranges:
            total_count = 0
            # start_row is inclusive >=
            # end_row is exclusive <
            if start_key or stop_key:
                if not start_key:
                    srow = None
                else:
                    srow = make_start_key(start_key, key_spec=key_spec)
                    #srow = _string_decrement(srow)
                if not stop_key:
                    erow = None
                else:
                    erow = make_end_key(stop_key, key_spec=key_spec)
                scanner = table.scan(row_start=srow, row_stop=erow)
            else:
                scanner = table.scan()

            for row in scanner:
                total_count += 1
                val = row[1]['d:d']
                yield deserialize_key(row[0], key_spec), val
                num_keys += 1
                keys_size += len(row[0])
                values_size += len(val)

        end_time = time.time()
        num_values = num_keys
        self.log_scan(table_name, start_time, end_time, num_keys,
                      keys_size, num_values, values_size)

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        '''Scan only the keys from a table.

        Yield key tuples from querying table_name for items with keys
        within the specified ranges.  If no key_ranges are provided,
        then yield all keys in table.

        This is equivalent to::

            itertools.imap(lambda (k,v): k,
                           self.scan(table_name, *key_ranges, **kwargs))

        But it avoids copying the (potentially large) data values across
        the network.

        :param str table_name: name of table to scan
        :param key_ranges: (`start`,`end`) key range pairs

        '''
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        values_size = 0
        key_spec = self._table_names[table_name]
        if not key_ranges:
            key_ranges = [['', '']]
        table = self.conn.table(table_name)
        for start_key, stop_key in key_ranges:
            total_count = 0
            # start_row is inclusive >=
            # end_row is exclusive <
            if start_key or stop_key:
                if not start_key:
                    srow = None
                else:
                    srow = make_start_key(start_key, key_spec=key_spec)
                    #srow = _string_decrement(srow)
                if not stop_key:
                    erow = None
                else:
                    erow = make_end_key(stop_key, key_spec=key_spec)
                scanner = table.scan(row_start=srow, row_stop=erow, columns=())
            else:
                scanner = table.scan(columns=())

            for row in scanner:
                total_count += 1
                yield deserialize_key(row[0], key_spec)
                num_keys += 1
                keys_size += len(row[0])

        end_time = time.time()
        self.log_scan_keys(table_name, start_time, end_time, num_keys, keys_size)


    def get(self, table_name, *keys, **kwargs):
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        num_values = 0
        values_size = 0

        key_spec = self._table_names[table_name]
        table = self.conn.table(table_name)
        for key in keys:
            skey = serialize_key(key, key_spec)
            keys_size += len(skey)
            num_keys += 1
            cf = table.row(skey, columns=['d:d'])
            val = cf.get('d:d', None)
            if val is not None:
                num_values += 1
                values_size += len(val)
            yield key, val
            ## TODO: is .row or .cells faster?
            # values = table.cells(skey, 'd:d', versions=1)
            # if values:
            #     val = values[0]
            #     yield key, val
            #     num_values += 1
            #     values_size += len(val)
            # else:
            #     yield key, None

        end_time = time.time()
        self.log_get(table_name, start_time, end_time, num_keys,
                     keys_size, num_values, values_size)

    def delete(self, table_name, *keys, **kwargs):
        key_spec = self._table_names[table_name]
        table = self.conn.table(table_name)
        batch = table.batch()
        for key in keys:
            batch.delete(serialize_key(key, key_spec))
        batch.send()

    def close(self):
        if self._connected:
            self._connected = False
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()


def addr_port(addr, default_port):
    if ':' in addr:
        host, port = addr.split(':')
        return host, int(port)
    else:
        return addr, default_port


def _string_decrement(x):
    if len(x) < 1:
        return None  # None is before all keys, aka negative infinity
    pre = x[:-1]
    post = ord(x[-1])
    if post > 0:
        return pre + chr(post - 1) + '\xff'
    else:
        pre = _string_decrement(pre)
        if pre is None:
            return None
        return pre + '\xff'
