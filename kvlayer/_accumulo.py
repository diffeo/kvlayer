'''Implementation of AbstractStorage using Apache Accumulo; requires the
Accumulo Thrift Proxy that is available in Accumulo 1.4.4+

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''

from __future__ import absolute_import
import logging
import random
import re
import time

from pyaccumulo import Accumulo, Mutation, Range, BatchWriter
from pyaccumulo.iterators import RowDeletingIterator
from pyaccumulo.proxy.ttypes import IteratorScope, \
    AccumuloSecurityException, IteratorSetting

from kvlayer._abstract_storage import StringKeyedStorage
from kvlayer._decorators import retry
from kvlayer._exceptions import ProgrammerError

logger = logging.getLogger(__name__)


class AStorage(StringKeyedStorage):
    '''
    Accumulo storage implements kvlayer's AbstractStorage, which
    manages a set of tables as specified to setup_namespace
    '''
    def __init__(self, *args, **kwargs):
        super(AStorage, self).__init__(*args, **kwargs)

        self._connected = False
        addresses = self._config.get('storage_addresses', [])
        if not addresses:
            raise ProgrammerError('config lacks storage_addresses')

        def str_to_pair(address):
            if ':' not in address:
                return (address, 50096)
            else:
                h, p = address.split(':')
                return (h, int(p))
        self._addresses = map(str_to_pair, addresses)

        # The following are all parameters to the accumulo
        # batch interfaces.
        self._max_memory = self._config.get('accumulo_max_memory', 1000000)
        self._timeout_ms = self._config.get('accumulo_timeout_ms', 30000)
        self._threads = self._config.get('accumulo_threads', 10)
        self._latency_ms = self._config.get('accumulo_latency_ms', 10)

        # Acumulo storage requires a username and password
        self._user = self._config.get('username', None)
        self._password = self._config.get('password', None)
        if not self._user and self._password:
            raise ProgrammerError('accumulo storage requires '
                                  'username/password')

        self.thrift_framed_transport_size_in_mb = \
            self._config['thrift_framed_transport_size_in_mb']

        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            host, port = random.choice(self._addresses)
            logger.debug('connecting to Accumulo %s:%d',
                         host, port)
            self._conn = Accumulo(host, port,
                                  self._user, self._password)
            self._connected = True
        return self._conn

    def _ns(self, table):
        '''
        accumulo does not have "namespaces" as a concept per se, so we
        achieve the same effect by creating tables with the namespace
        string appended to the end of the table name
        '''
        return '%s_%s_%s' % (self._app_name, self._namespace, table)

    def _create_table(self, table):
        ns_table = self._ns(table)
        if self.conn.table_exists(ns_table):
            logger.info('table %s exists, not modifying', ns_table)
            return
        logger.info('creating %s', ns_table)

        self.conn.create_table(ns_table)
        logger.debug('conn.created_table(%s)', ns_table)
        self.conn.client.setTableProperty(self.conn.login,
                                          ns_table,
                                          'table.bloom.enabled',
                                          'true')

        i = RowDeletingIterator()
        scopes = set([IteratorScope.SCAN, IteratorScope.MINC,
                      IteratorScope.MAJC])
        i.attach(self.conn, ns_table, scopes)
        logger.debug('i.attach(%r, %s, %r)', self.conn, ns_table, scopes)

    def setup_namespace(self, table_names, value_types={}):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        super(AStorage, self).setup_namespace(table_names, value_types)
        for table in table_names:
            if not self.conn.table_exists(self._ns(table)):
                self._create_table(table)

    def delete_namespace(self):
        '''
        delete all of the tables within namespace
        '''
        logger.debug('getting list of tables')
        tables = self.conn.list_tables()
        logger.debug('finding tables to delete from %s: %r',
                     self._ns(''), tables)
        tables_to_delete = [x for x in tables if re.search(self._ns(''), x)]
        for table in tables_to_delete:
            self.conn.delete_table(table)

    def clear_table(self, table_name):
        self.conn.delete_table(self._ns(table_name))
        self._create_table(table_name)

    @retry([AccumuloSecurityException])
    def _put(self, table_name, keys_and_values):
        cur_bytes = 0
        max_bytes = self.thrift_framed_transport_size_in_mb * 2 ** 19
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        try:
            for key, blob in keys_and_values:
                if len(blob) + cur_bytes >= max_bytes:
                    logger.debug('len(blob)=%d + cur_bytes=%d >= '
                                 'thrift_framed_transport_size_in_mb/2 = %d',
                                 len(blob), cur_bytes, max_bytes)
                    logger.debug('pre-emptively sending only what has been '
                                 'batched, and will send this item in next '
                                 'batch.')
                    batch_writer.flush()
                    cur_bytes = 0
                cur_bytes += max_bytes
                mut = Mutation(key)
                mut.put(cf='', cq='', val=blob)
                batch_writer.add_mutation(mut)
        finally:
            batch_writer.close()

    def _scan(self, table_name, key_ranges):
        for kv in self._do_scan(table_name, key_ranges, keys_only=False):
            yield kv

    def _scan_keys(self, table_name, key_ranges):
        for kv in self._do_scan(table_name, key_ranges, keys_only=True):
            yield kv

    def _do_scan(self, table_name, key_ranges, keys_only):
        key_spec = self._table_names[table_name]
        iterators = []
        if keys_only:
            iterators.append(IteratorSetting(
                name='SortedKeyIterator', priority=10,
                iteratorClass='org.apache.accumulo.core.iterators.'
                'SortedKeyIterator', properties={}))
        if not key_ranges:
            key_ranges = [['', '']]
        for start_key, stop_key in key_ranges:
            total_count = 0
            specific_key_range = bool(start_key or stop_key)
            if specific_key_range:
                # Accumulo treats None as a negative-infinity or
                # positive-infinity key as needed for starts and ends
                # of ranges.
                #
                # pyaccumulo has a bug at present 20140228_171555
                # which causes it to never do a '>=' scan, so we must
                # decrement the start key to include the start key
                # which necessary for a get() operation.
                # https://github.com/accumulo/pyaccumulo/issues/14
                if start_key:
                    start_key = _string_decrement(start_key)
                key_range = Range(srow=start_key, erow=stop_key,
                                  sinclude=True, einclude=True)
                scanner = self.conn.scan(self._ns(table_name),
                                         scanrange=key_range,
                                         iterators=iterators)
            else:
                scanner = self.conn.scan(self._ns(table_name),
                                         iterators=iterators)

            for row in scanner:
                if keys_only:
                    yield row.row
                else:
                    yield (row.row, row.val)

    def _get(self, table_name, keys):
        for key in keys:
            gen = self._do_scan(table_name, [(key, key)], keys_only=False)
            v = None
            for kk, vv in gen:
                if kk == key:
                    v = vv
            yield key, v

    def close(self):
        self._connected = False
        if hasattr(self, 'pool') and self.pool:
            self.pool.dispose()

    def __del__(self):
        self.close()

    def _delete(self, table_name, keys):
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        try:
            for key in keys:
                mut = Mutation(key)
                mut.put(cf='', cq='', val='DEL_ROW')
                batch_writer.add_mutation(mut)
        finally:
            batch_writer.close()


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
