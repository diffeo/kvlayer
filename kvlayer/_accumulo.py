'''Apache Accumulo kvlayer backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging
import random
import re

from pyaccumulo import Accumulo, Mutation, Range, BatchWriter
from pyaccumulo.iterators import RowDeletingIterator
from pyaccumulo.proxy.ttypes import IteratorScope, \
    AccumuloSecurityException, IteratorSetting

from kvlayer._decorators import retry
from kvlayer._exceptions import ProgrammerError
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import split_key, make_start_key, make_end_key, \
    join_key_fragments

logger = logging.getLogger('kvlayer')


class AStorage(AbstractStorage):
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
                h,p = address.split(':')
                return (h, int(p))
        self._addresses = map(str_to_pair, addresses)

        ## The following are all parameters to the accumulo
        ## batch interfaces.
        self._max_memory = self._config.get('accumulo_max_memory', 1000000)
        self._timeout_ms = self._config.get('accumulo_timeout_ms', 30000)
        self._threads = self._config.get('accumulo_threads', 10)
        self._latency_ms = self._config.get('accumulo_latency_ms', 10)

        ## Acumulo storage requires a username and password
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
        logger.info('creating %s', self._ns(table))

        self.conn.create_table(self._ns(table))
        logger.debug('conn.created_table(%s)', self._ns(table))
        self.conn.client.setTableProperty(self.conn.login,
                                          self._ns(table),
                                          'table.bloom.enabled',
                                          'true')
        logger.debug("conn.client.setTableProperty(%r, %s, table.bloom.enabled', 'true'",
                     self.conn.login, self._ns(table))

        i = RowDeletingIterator()
        scopes = set([IteratorScope.SCAN, IteratorScope.MINC,
                      IteratorScope.MAJC])
        i.attach(self.conn, self._ns(table), scopes)
        logger.debug('i.attach(%r, %s, %r)', self.conn, self._ns(table), scopes)

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        super(AStorage, self).setup_namespace(table_names)
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
        self.conn.create_table(self._ns(table_name))

    @retry([AccumuloSecurityException])
    def put(self, table_name, *keys_and_values, **kwargs):
        key_spec = self._table_names[table_name]
        cur_bytes = 0
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        for key, blob in keys_and_values:
            self.check_put_key_value(key, blob, table_name, key_spec)
            if (len(blob) + cur_bytes >=
                    self.thrift_framed_transport_size_in_mb * 2 ** 19):
                logger.debug('len(blob)=%d + cur_bytes=%d >= '
                             'thrift_framed_transport_size_in_mb/2 = %d',
                             len(blob), cur_bytes,
                             self.thrift_framed_transport_size_in_mb * 2 ** 19)
                logger.debug('pre-emptively sending only what has been '
                             'batched, and will send this item in next batch.')
                batch_writer.flush()
                cur_bytes = 0
            mut = Mutation(join_key_fragments(key, key_spec=key_spec))
            mut.put(cf='', cq='', val=blob)
            batch_writer.add_mutation(mut)
            cur_bytes += len(blob)
        batch_writer.close()

    def scan(self, table_name, *key_ranges, **kwargs):
        return self._scan(table_name, key_ranges, keys_only=False)

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        return self._scan(table_name, key_ranges, keys_only=True)

    def _scan(self, table_name, key_ranges, keys_only):
        key_spec = self._table_names[table_name]
        iterators=[]
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
                if not start_key:
                    srow = None
                else:
                    srow = make_start_key(start_key, key_spec=key_spec)
                    srow = _string_decrement(srow)
                if not stop_key:
                    erow = None
                else:
                    erow = make_end_key(stop_key, key_spec=key_spec)
                key_range = Range(srow=srow, erow=erow, sinclude=True, einclude=True)
                scanner = self.conn.scan(self._ns(table_name),
                                         scanrange=key_range,
                                         iterators=iterators)
            else:
                scanner = self.conn.scan(self._ns(table_name),
                                         iterators=iterators)

            for row in scanner:
                total_count += 1
                key = split_key(row.row, key_spec)
                if keys_only:
                    yield key
                else:
                    yield key, row.val

    def get(self, table_name, *keys, **kwargs):
        for key in keys:
            gen = self.scan(table_name, (key, key))
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

    def delete(self, table_name, *keys, **kwargs):
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        for key in keys:
            mut = Mutation(join_key_fragments(key, key_spec=self._table_names[table_name]))
            mut.put(cf='', cq='', val='DEL_ROW')
            batch_writer.add_mutation(mut)
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
