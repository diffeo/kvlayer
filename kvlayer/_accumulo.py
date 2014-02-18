'''
Implementation of AbstractStorage using Cassandra

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import re
import logging
from kvlayer._decorators import retry
from kvlayer._exceptions import MissingID, ProgrammerError
from kvlayer._abstract_storage import AbstractStorage
from pyaccumulo import Accumulo, Mutation, Range, BatchWriter
from pyaccumulo.iterators import RowDeletingIterator
from pyaccumulo.proxy.ttypes import IteratorScope, AccumuloSecurityException
from _utils import join_uuids, split_uuids

logger = logging.getLogger('kvlayer')


class AStorage(AbstractStorage):
    '''
    Accumulo storage implements kvlayer's AbstractStorage, which
    manages a set of tables as specified to setup_namespace
    '''
    def __init__(self, config):
        super(AStorage, self).__init__(config)

        self._connected = False
        addresses = config.get('storage_addresses', [])
        if not addresses:
            raise ProgrammerError('config lacks storage_addresses')

        logger.info('accumulo thrift proxy supports only one address, using '
                    'first: %r' % addresses)
        address = addresses[0]
        if ':' not in address:
            self._host = address
            self._port = 50096
        else:
            self._host, self._port = address.split(':')
            self._port = int(self._port)

        ## The following are all parameters to the accumulo
        ## batch interfaces.
        self._max_memory = config.get('accumulo_max_memory', 1000000)
        self._timeout_ms = config.get('accumulo_timeout_ms', 30000)
        self._threads = config.get('accumulo_threads', 10)
        self._latency_ms = config.get('accumulo_latency_ms', 10)

        ## Acumulo storage requires a username and password
        self._user = config.get('username', None)
        self._password = config.get('password', None)
        if not self._user and self._password:
            raise ProgrammerError('accumulo storage requires '
                                  'username/password')

        self.thrift_framed_transport_size_in_mb = \
            config['thrift_framed_transport_size_in_mb']

        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            logger.critical('connecting to Accumulo')
            self._conn = Accumulo(self._host, self._port,
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
        logger.debug('creating tables: %r', table_names)
        self._table_names.update(table_names)
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
        num_uuids = self._table_names[table_name]
        cur_bytes = 0
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        for key, blob in keys_and_values:
            ex = self.check_put_key_value(key, blob, table_name, num_uuids)
            if ex:
                raise ex
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
            mut = Mutation(join_uuids(*key))
            mut.put(cf='', cq='', val=blob)
            batch_writer.add_mutation(mut)
            cur_bytes += len(blob)
        batch_writer.close()

    def scan(self, table_name, *key_ranges, **kwargs):
        num_uuids = self._table_names[table_name]
        if not key_ranges:
            key_ranges = [['', '']]
        for start_key, stop_key in key_ranges:
            total_count = 0
            specific_key_range = bool(start_key or stop_key)
            if specific_key_range:
                joined_key = join_uuids(*start_key, num_uuids=num_uuids,
                                        padding='0')
                ## If there is no start key we need to scan from the beginning of the range.  To include any value
                ## stored at UUID(int=0) we need a key that occurs before the key with all zeros.  The key we use
                ## to accomplish this is '.' just like the _preceeding_key function.
                srow = len(start_key)>0 and self._preceeding_key(joined_key) or '.'
                erow = len(stop_key)>0 and join_uuids(*stop_key, num_uuids=num_uuids, padding='f') or 'f' * 32 * num_uuids
                key_range = Range(srow=srow, erow=erow)
                scanner = self.conn.scan(self._ns(table_name),
                                         scanrange=key_range)
            else:
                scanner = self.conn.scan(self._ns(table_name))

            for row in scanner:
                total_count += 1
                yield tuple(split_uuids(row.row)), row.val
            else:
                if specific_key_range and total_count == 0:
                    raise MissingID('table_name=%r start=%r finish=%r' % (
                                    table_name, start_key, stop_key))

    def get(self, table_name, *keys, **kwargs):
        for key in keys:
            gen = self.scan(table_name, (key, key))
            yield gen.next()

    def close(self):
        self._connected = False
        if hasattr(self, 'pool') and self.pool:
            self.pool.dispose()

    def __del__(self):
        self.close()

    def _preceeding_key(self, key):
        num = (int(key, 16) - 1)
        if num < 0:
            return '.'
        format_string = '%%0.%dx' % len(key)
        return format_string % num

    def delete(self, table_name, *keys, **kwargs):
        batch_writer = BatchWriter(conn=self.conn,
                                   table=self._ns(table_name),
                                   max_memory=self._max_memory,
                                   latency_ms=self._latency_ms,
                                   timeout_ms=self._timeout_ms,
                                   threads=self._threads)
        for key in keys:
            mut = Mutation(join_uuids(*key))
            mut.put(cf='', cq='', val='DEL_ROW')
            batch_writer.add_mutation(mut)
        batch_writer.close()
