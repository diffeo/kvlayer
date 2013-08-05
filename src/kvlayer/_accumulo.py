'''
Implementation of AbstractStorage using Cassandra

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import re
import logging
from kvlayer._exceptions import MissingID
from kvlayer._abstract_storage import AbstractStorage
from pyaccumulo import Accumulo, Mutation, Range, BatchWriter
from _utils import join_uuids, split_uuids

logger = logging.getLogger('__name__')


class AStorage(AbstractStorage):
    """
    Accumulo storage implements two column families within a namespace
    specified by the user:

       * 'tree' stores VertexFamilies of parents and their children.

       * 'inbound' stores binary blobs of vertex data awaiting
         insertion into the tree

       * 'events' stores log messages

       # 'meta' stores meta data information

    """
    def __init__(self, config):
        '''
        thrift proxy server for accumulo doesn't currently
        support more than one address
        assert len(config['storage_addresses']) == 1
        storage_host_port = config['storage_addresses'][0]
        host, port = storage_host_port.split(':', 1)
        '''
        self._connected = False
        self._host = config['host']
        self._port = config['port']
        self._user = config['user']
        self._password = config['password']
        self._namespace = ''
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = Accumulo(self._host, self._port,
                                  self._user, self._password)
            self._connected = True
        return self._conn

    def _ns(self, table):
        return '%s_%s' % (table, self._namespace)

    def setup_namespace(self, namespace, table_names):
        self._namespace = namespace
        for table in table_names:
            if not self.conn.table_exists(self._ns(table)):
                self.conn.create_table(self._ns(table))

    def delete_namespace(self, namespace):
        tables = self.conn.list_tables()
        tables_to_delete = [x for x in tables if re.search(namespace, x)]
        for table in tables_to_delete:
            self.conn.delete_table(table)

    def clear_table(self, table_name):
        self.conn.delete_table(self._ns(table_name))
        self.conn.create_table(self._ns(table_name))

    def create_if_missing(self, namespace, table_name, num_uuids):
        self._namespace = namespace
        if not self.conn.table_exists(self._ns(table_name)):
            self.conn.create_table(self._ns(table_name))

    def put(self, table_name, *keys_and_values, **kwargs):
        batch_writer = BatchWriter(conn=self.conn, table=self._ns(table_name),
                                   max_memory=10, latency_ms=30,
                                   timeout_ms=5, threads=11)
        for key, value in keys_and_values:
            mut = Mutation(join_uuids(*key))
            mut.put(cf="cf1", cq="cq1", val=value)
            batch_writer.add_mutation(mut)
        batch_writer.close()

    def get(self, table_name, *key_ranges, **kwargs):
        for start_key, stop_key in key_ranges:
            try:
                key_range = Range(srow=self._preceeding_key(join_uuids(*start_key)),
                                  erow=join_uuids(*stop_key))
                scanner = self.conn.scan(self._ns(table_name),
                                         scanrange=key_range)
                row = scanner.next()
                yield tuple(split_uuids(row.row)), row.val
            except ValueError: # FIX with correct exception
                raise MissingID()

    def _preceeding_key(self, key):
        format_string = '%%0.%dx' % len(key)
        return format_string % (int(key, 16) - 1)

    def delete(self, table_name, *keys, **kwargs):
        for key in keys:
            joined_key = join_uuids(*key)
            preceeding_key = self._preceeding_key(joined_key)
            self.conn.client.deleteRows(self.conn.login,
                                        self._ns(table_name),
                                        preceeding_key,
                                        joined_key)

    def close(self):
        self._connected = False
