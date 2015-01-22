'''
Implementation of AbstractStorage using HBase

Your use of this software is governed by your license agreement.

Copyright 2012-2015 Diffeo, Inc.
'''

import logging
import random

import happybase as hbase

from kvlayer._exceptions import ProgrammerError
from kvlayer._abstract_storage import StringKeyedStorage

logger = logging.getLogger(__name__)


class HBaseStorage(StringKeyedStorage):
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

        self.max_batch_bytes = int(self._config.get('max_batch_bytes',
                                                    10000000))

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

    def setup_namespace(self, table_names, value_types={}):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace. This operation is idempotent.
        '''
        super(HBaseStorage, self).setup_namespace(table_names, value_types)
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

    def _put(self, table_name, keys_and_values):
        cur_bytes = 0
        table = self.conn.table(table_name)
        batch = table.batch()
        for key, blob in keys_and_values:
            morelen = len(blob) + len(key)
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
            batch.put(key, {'d:d': blob})
            cur_bytes += morelen
        if cur_bytes > 0:
            batch.send()

    def _scan(self, table_name, key_ranges):
        if not key_ranges:
            key_ranges = [['', '']]
        table = self.conn.table(table_name)
        for start_key, stop_key in key_ranges:
            # start_row is inclusive >=
            # end_row is exclusive <
            if start_key or stop_key:
                scanner = table.scan(row_start=start_key, row_stop=stop_key)
            else:
                scanner = table.scan()

            for row in scanner:
                yield (row[0], row[1]['d:d'])

    def _scan_keys(self, table_name, key_ranges):
        if not key_ranges:
            key_ranges = [['', '']]
        table = self.conn.table(table_name)
        for start_key, stop_key in key_ranges:
            # start_row is inclusive >=
            # end_row is exclusive <
            if start_key or stop_key:
                scanner = table.scan(row_start=start_key, row_stop=stop_key,
                                     columns=())
            else:
                scanner = table.scan(columns=())

            for row in scanner:
                yield row[0]

    def _get(self, table_name, keys):
        table = self.conn.table(table_name)
        for key in keys:
            cf = table.row(key, columns=['d:d'])
            val = cf.get('d:d', None)
            yield key, val
            # TODO: is .row or .cells faster?
            # values = table.cells(skey, 'd:d', versions=1)
            # if values:
            #     val = values[0]
            #     yield key, val
            #     num_values += 1
            #     values_size += len(val)
            # else:
            #     yield key, None

    def _delete(self, table_name, keys):
        table = self.conn.table(table_name)
        batch = table.batch()
        for key in keys:
            batch.delete(key)
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
