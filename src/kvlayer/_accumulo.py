'''
Implementation of AbstractStorage using Cassandra

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import re
import logging
from kvlayer._exceptions import MissingID, ProgrammerError
from kvlayer._abstract_storage import AbstractStorage
from pyaccumulo import Accumulo, Mutation, Range, BatchWriter
from _utils import join_uuids, split_uuids

logger = logging.getLogger('kvlayer')


class AStorage(AbstractStorage):
    '''
    Accumulo storage implements kvlayer's AbstractStorage, which
    manages a set of tables as specified to setup_namespace
    '''
    def __init__(self, config):
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

        self.table_names = {}
        self._user = config.get('username', None)
        self._password = config.get('password', None)
        if not self._user and self._password:
            raise ProgrammerError('accumulo storage requires '
                                  'username/password')

        self._namespace = config.get('namespace', None)
        if not self._namespace:
            raise ProgrammerError('kvlayer requires a namespace')

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
        return '%s_%s' % (table, self._namespace)

    def setup_namespace(self, namespace, table_names):
        '''
        create tables within the namespace
        '''
        self._namespace = namespace
        logger.info('creating tables')
        self.table_names = table_names
        for table in table_names:
            if not self.conn.table_exists(self._ns(table)):
                logger.info('creating accumulo table for %s: %r' %
                            (namespace, table))
                self.conn.create_table(self._ns(table))
                self.conn.client.setTableProperty(self.conn.login,
                                                  self._ns(table),
                                                  'table.bloom.enabled',
                                                  'true')

    def delete_namespace(self, namespace):
        '''
        delete all of the tables within namespace
        '''
        logger.critical('getting list of tables')
        tables = self.conn.list_tables()
        logger.critical('searching through tables to find deletes for '
                        '%s: %r' % (namespace, tables))
        tables_to_delete = [x for x in tables if re.search(namespace, x)]
        for table in tables_to_delete:
            self.conn.delete_table(table)

    def clear_table(self, table_name):
        self.conn.delete_table(self._ns(table_name))
        self.conn.create_table(self._ns(table_name))

    def create_if_missing(self, namespace, table_name, num_uuids):
        self._namespace = namespace
        self.table_names[table_name] = num_uuids
        if not self.conn.table_exists(self._ns(table_name)):
            self.conn.create_table(self._ns(table_name))
            self.conn.client.setTableProperty(self.conn.login,
                                              self._ns(table_name),
                                              'table.bloom.enabled',
                                              'true')

    def put(self, table_name, *keys_and_values, **kwargs):
        batch_writer = BatchWriter(conn=self.conn, table=self._ns(table_name),
                                   max_memory=1000000, latency_ms=10,
                                   timeout_ms=1000, threads=10)
        for key, value in keys_and_values:
            mut = Mutation(join_uuids(*key))
            mut.put(cf="", cq="", val=value)
            batch_writer.add_mutation(mut)
        batch_writer.close()

    def get(self, table_name, *key_ranges, **kwargs):
        num_uuids = self.table_names[table_name]
        if not key_ranges:
            key_ranges = [['', '']]
        for start_key, stop_key in key_ranges:
            total_count = 0
            specific_key_range = bool(start_key or stop_key)
            if specific_key_range:
                joined_key = join_uuids(*start_key, num_uuids=num_uuids,
                                        padding='0')
                srow = self._preceeding_key(joined_key)
                erow = join_uuids(*stop_key, num_uuids=num_uuids, padding='f')
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

    def _preceeding_key(self, key):
        num = (int(key, 16) - 1)
        if num < 0:
            return '.'
        format_string = '%%0.%dx' % len(key)
        return format_string % num

    def delete(self, table_name, *keys, **kwargs):
        for key in keys:
            joined_key = join_uuids(*key)
            preceeding_key = self._preceeding_key(joined_key)
            logger.debug('delete %s from %s' % (str(key), table_name))
            self.conn.client.deleteRows(self.conn.login,
                                        self._ns(table_name),
                                        preceeding_key,
                                        joined_key)

    def close(self):
        self._connected = False
