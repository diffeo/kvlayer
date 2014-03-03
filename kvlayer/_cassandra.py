'''
Implementation of AbstractStorage using Cassandra

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

import uuid
import time
import random
import logging
import traceback
from collections import defaultdict
from kvlayer._utils import join_uuids, split_uuids, make_start_key, make_end_key, join_key_fragments
from kvlayer._exceptions import MissingID
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection
from thrift.transport.TTransport import TTransportException

logger = logging.getLogger('kvlayer.CStorage')

## get the Cassandra client library
import pycassa
from pycassa import NotFoundException
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
    ASCII_TYPE, BYTES_TYPE
from pycassa.types import AsciiType


class LittleLogger(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, dic):
        #logger.debug('event: %s %r' % (self.name, dic))
        return

class _PycassaListener(object):
    def __init__(self, storage):
        self.storage = storage
        for method_name in ['connection_checked_out', 'connection_created', 'connection_disposed', 'connection_recycled', 'pool_at_max',
                            'pool_disposed', 'server_list_obtained', 'connection_checked_in']:
            setattr(self, method_name, LittleLogger(method_name))

    def connection_failed(self, dic):
        logger.critical('connection_failed: %s:' % str(dic))
        #self.storage.recreate_pool()


class CStorage(AbstractStorage):
    """
    Cassandra storage implements a set of table-like structures using
    ColumnFamilies in a namespace:

    http://www.slideshare.net/edanuff/indexing-in-cassandra

    """
    def __init__(self):
        super(CStorage, self).__init__()
        self.storage_addresses = self._config['storage_addresses']
        self.max_consistency_delay = self._config.get('max_consistency_delay', 120)
        ## avoid switching servers when creating/deleting namespaces
        self._chosen_server = random.choice(self.storage_addresses)
        logger.info('CStorage(_chosen_server=%r', self._chosen_server)
        self.pool_size = self._config.get('connection_pool_size', 2)
        self._connected = False
        self.thrift_framed_transport_size_in_mb = self._config.get('thrift_framed_transport_size_in_mb', 15)
        self.pool = None
        self.tables = {}
        self._app_namespace = self._app_name + '_' + self._namespace

    def setup_namespace(self, table_names):
        if self.pool:
            self.pool.dispose()
            del self.pool
            self.pool = None
        start_connect_time = time.time()

        self._table_names.update(table_names)

        sm = SystemManager(self._chosen_server)
        try:
            sm.create_keyspace(
                self._app_namespace, SIMPLE_STRATEGY,
                {
                    'replication_factor': str(self._config.get('replication_factor', '1'))
                },
                )
        except pycassa.InvalidRequestException, exc:
            if exc.why.startswith('Keyspace names must be case-insensitively unique'):
                pass
            else:
                raise exc

        self._create_tables(table_names, sm=sm)
        self.wait_for_consistency(sm=sm)

        ## now that we are consistent, we can create a pool
        self.pool = ConnectionPool(self._app_namespace, self.storage_addresses,
                          max_retries=1000, pool_timeout=10, pool_size=2, timeout=120)
        self.pool.fill()
        self.pool.add_listener(_PycassaListener(self))

        for family in self._table_names:
            if family not in self.tables:
                self.tables[family] = self._get_cf(family)

        elapsed = time.time() - start_connect_time
        logger.info('took %.3f seconds to setup_namespace() ConnectionPool(%d)' % (
                elapsed, self.pool_size))

        ## indicated connection is established
        self._connected = True

    def _get_cf(self, cf_name):
        return pycassa.ColumnFamily(
                self.pool, cf_name,
                read_consistency_level=pycassa.ConsistencyLevel.ALL,
                write_consistency_level=pycassa.ConsistencyLevel.ALL,
                )

    def _create_tables(self, table_names, sm=None):
        if sm is None:
            sm = SystemManager(self._chosen_server)
        for family, num_uuids in table_names.items():
            #CompositeType(*[UUIDType() for i in range(num_uuids)])
            comparator = AsciiType()
            try:
                sm.create_column_family(
                    self._app_namespace, family, super=False,
                    key_validation_class = ASCII_TYPE,
                    default_validation_class = BYTES_TYPE,
                    comparator_type=comparator,
                    )
            except pycassa.InvalidRequestException, exc:
                if exc.why.startswith('Cannot add already existing column family'):
                    pass
                else:
                    raise exc

    def wait_for_consistency(self, sm=None):
        if sm is None:
            sm = SystemManager(self._chosen_server)
        start_consistency_delay = time.time()
        consistency_delay = 0
        while len(sm.describe_schema_versions()) > 1 and \
                consistency_delay < self.max_consistency_delay:
            consistency_delay = time.time() - start_consistency_delay
            if consistency_delay > 20:
                logger.warn('waited %.1f seconds for cluster-wide consistency %r' % (
                        consistency_delay, sm.describe_schema_versions()))
            time.sleep(0.2)
        logger.info('number of schemas in cluster: %d' % len(sm.describe_schema_versions()))

    def delete_namespace(self):
        sm = SystemManager(self._chosen_server)
        try:
            sm.drop_keyspace(self._app_namespace)
        except pycassa.InvalidRequestException, exc:
            if exc.why.startswith('Cannot drop non existing keyspace'):
                pass
            else:
                raise exc
        except TTransportException, exc:
            logger.critical('trapping: %s' % traceback.format_exc(exc))

        sm.close()

    def _shard_number(self, joined_key):
        ## use first three hex characters, i.e. 4096 shards
        return int(joined_key[:3], 16)

    def _make_shard_name(self, table_name, joined_key):
        '''
        create a prefix-based sharding of table_name across the C* cluster
        '''
        return '%s-%04d' % (table_name, self._shard_number(joined_key))

    def _make_shard_names(self, table_name, start, finish):
        '''
        generate all row names (shards) needed between start and finish
        '''
        start_shard  = self._shard_number(start)
        finish_shard = self._shard_number(finish)
        row_names = []
        for shard_num in range(start_shard, finish_shard + 1):
            row_names.append('%s-%04d' % (table_name, shard_num))
        return row_names

    @_requires_connection
    def clear_table(self, table_name):
        self.tables[table_name].truncate()

    @_requires_connection
    def put(self, table_name, *keys_and_values, **kwargs):
        batch_size = kwargs.pop('batch_size', None)
        tot_bytes = 0
        cur_bytes = 0
        tot_rows = 0
        cur_rows = 0
        num_uuids = self._table_names[table_name]
        start = time.time()
        logger.debug('starting save')
        batch = self.tables[table_name].batch(queue_size=batch_size)
        for key, blob in keys_and_values:
            ex = self.check_put_key_value(key, blob, table_name, num_uuids)
            if ex:
                raise ex
            if len(blob) + cur_bytes >= self.thrift_framed_transport_size_in_mb * 2**19:
                logger.critical('len(blob)=%d + cur_bytes=%d >= thrift_framed_transport_size_in_mb/2 = %d'
                                % (len(blob), cur_bytes, self.thrift_framed_transport_size_in_mb * 2**19))
                ## convert to MB and then cut in half
                if cur_rows > 0:
                    logger.critical('pre-emptively sending only what has been batched, and will send this item in next batch.')
                    batch.send()
                cur_bytes = 0
                cur_rows = 0
            cur_bytes += len(blob)
            tot_bytes += len(blob)
            cur_rows += 1
            tot_rows += 1
            if not isinstance(key, tuple):
                ## for consistency, always make things join_uuids,
                ## even if only one
                key = (key,)
            joined_key = join_uuids(*key)
            row_name = self._make_shard_name(table_name, joined_key)
            if len(blob) >= self.thrift_framed_transport_size_in_mb * 2**19:
                logger.critical('len(blob)=%d >= thrift_framed_transport_size_in_mb / 2 = %d, so there is a risk that the total payload will exceed the full thrift_framed_transport_size_in_mb, and the only solution to this is to change Cassandra server-side config to allow larger frames...'
                                % (len(blob), self.thrift_framed_transport_size_in_mb * 2**19))
            batch.insert(row_name, {joined_key: blob})
            #logger.critical('saving %s %r %r' % (table_name, key, blob))
            if tot_rows % 500 == 0:
                logger.debug('num rows=%d, num MB=%d, thrift_framed_transport_size_in_mb=%d' % (
                    tot_rows, float(tot_bytes) / 2**20, self.thrift_framed_transport_size_in_mb))

        batch.send()

        elapsed = time.time() - start
        row_rate = float(tot_rows)  / elapsed
        MB_rate  = float(tot_bytes) / elapsed / 2**20

        logger.info('%s.insert(%d rows, %d bytes in %.1f sec --> %.1f rows/sec %.3f MBps' % (
                table_name, tot_rows, tot_bytes, elapsed, row_rate, MB_rate))

    @_requires_connection
    def scan(self, table_name, *key_ranges, **kwargs):
        kwargs.pop('batch_size', 100)
        if not key_ranges:
            ## get all columns
            key_ranges = [['', '']]
        num_uuids = self._table_names[table_name]
        for start, finish in key_ranges:
            specific_key_range = bool( start or finish )
            if specific_key_range and start == finish and len(start) == num_uuids:
                logger.warn('doing a scan on a single element, what?')
                #logger.info('specific_key_range: %r %r' % (start, finish))
                assert len(start) == num_uuids
                joined_key = join_uuids(*start)
                columns = [joined_key]
                row_names = [self._make_shard_name(table_name, joined_key)]
                start = None
                finish = None
            else:
                columns = None
                start = make_start_key(start, uuid_mode=self._require_uuid, num_uuids=num_uuids)
                finish = make_end_key(finish, uuid_mode=self._require_uuid, num_uuids=num_uuids)
                row_names = self._make_shard_names(table_name, start, finish)
            total_count = 0
            hit_empty = False
            for row_name in row_names:
                try:
                    for key, val in self._get_from_one_row(table_name, row_name, columns, start, finish, num_uuids):
                        assert len(key) == num_uuids
                        yield key, val
                        if start:
                            assert start <= join_uuids(*key)
                        if finish:
                            assert finish >= join_uuids(*key)

                        total_count += 1
                        #logger.critical('total_count: %d' % total_count)
                except pycassa.NotFoundException:
                    hit_empty = True

            #logger.debug('specific_key_range: %r  hit_empty: %r  total_count: %r' %
            #                (specific_key_range, hit_empty, total_count))

            if specific_key_range and hit_empty and total_count == 0:
                raise MissingID('table_name=%r columns=%r start=%r finish=%r' % (
                        table_name, columns, start, finish))

    def _get_from_one_row(self, table_name, row_name, columns, start, finish, num_uuids):

        logger.debug('c* get: table_name=%r row_name=%r columns=%r start=%r finish=%r' % (
            table_name, row_name, columns, start, finish))

        if not columns:
            assert start is not None and finish is not None
            assert start <= finish

        num_yielded = 0
        while True:
            ## if we have
            prev_start = start
            logger.debug('cassandra get(%r...)' % row_name)
            ## if table_name == 'inbound':
            ##     import ipdb
            ##     ipdb.set_trace()
            for key, val in self.tables[table_name].get(
                    row_name,
                    columns=columns,
                    column_start=start,
                    column_finish=finish,
                    column_count=1,
                    ).iteritems():

                key = split_uuids(key)
                logger.critical('cassandra get(%r) yielding %r %d' % (table_name, key, len(val)))
                yield key, val
                num_yielded += 1
                logger.debug('c* get: table_name=%r row_name=%r columns=%r start=%r finish=%r' % (
                    table_name, row_name, columns, start, finish))

            ## prepare to page ahead to next batch
            if columns:
                break
            start = list(key)
            start[-1] = uuid.UUID(int=key[-1].int+1)
            assert len(start) == num_uuids
            start = join_uuids(*start)

            if start == prev_start or start > finish:
                break
            logger.debug('paging forward from %r to %r' % (prev_start, start))

        ## We need to raise a not found exception if the caller asked for
        ## a specific column and we didn't yield any results
        if not columns and num_yielded == 0:
            raise pycassa.NotFoundException
            #'c* get: table_name=%r row_name=%r columns=%r start=%r finish=%r' % (
            #    table_name, row_name, columns, start, finish))


    @_requires_connection
    def get(self, table_name, *keys, **kwargs):
        num_uuids = self._table_names[table_name]
        ## A mapping between shards and columns to retrieve from each shard
        shards = defaultdict(list)

        ## Track the keys that we find
        found_keys = set()

        ## Determine all the shards that we need to contact
        for key in keys:
            assert len(key) == num_uuids
            joined_key = join_uuids(*key)
            row_names = [self._make_shard_name(table_name, joined_key)]
            for row_name in row_names:
                shards[row_name].append(joined_key)

        ## For each shard we contact, get all the keys it may hold
        for row_name, columns in shards.iteritems():
            try:
                for key, val in self.tables[table_name].get(row_name, columns=columns,).iteritems():
                    key = split_uuids(key)
                    logger.critical('cassandra get(%r) yielding %r %d' % (table_name, key, len(val)))
                    ## key is a list [(uuid, uuid)]
                    assert not any(k in found_keys for k in key)
                    found_keys.add(tuple(key))
                    yield tuple(key), val
            except NotFoundException:
                raise MissingID('table_name=%r keys: %r' % (table_name, columns))

        ## Raise an exception if we don't retrieve all the keys that were requested
        missing_keys = set(keys) - found_keys
        if missing_keys:
            raise MissingID('table_name=%r keys: %r' % ( table_name, missing_keys))


    @_requires_connection
    def delete(self, table_name, *keys, **kwargs):
        num_uuids = self._table_names[table_name]
        batch_size = kwargs.pop('batch_size', 1000)
        batch = self.tables[table_name].batch(queue_size=batch_size)
        count = 0
        for key in keys:
            assert len(key) == num_uuids
            joined_key = join_uuids(*key)
            row_name = self._make_shard_name(table_name, joined_key)
            columns = [joined_key]
            #logger.critical('C* delete: table_name=%r columns=%r' % (table_name, columns))
            batch.remove(row_name, columns=columns)
            count += 1
        batch.send()
        logger.info('deleted %d tree_ids from %r' % (count, table_name))

    def close(self):
        self._connected = True
        if hasattr(self, 'pool') and self.pool:
            self.pool.dispose()

    def __del__(self):
        self.close()
