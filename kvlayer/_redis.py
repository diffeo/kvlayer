"""Redis kvlayer storage implementation.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

Purpose
=======

This is an implementation of kvlayer that uses redis_ for its
underlying storage.  This generally limits storage to what can be held
in memory on the host system, but at the same time, redis is expected
to be available on many systems and requires much less setup than a
distributed Bigtable system.

Implementation Notes
====================

Redis is also used by rejester_ for its underlying storage.  Using
a different ``app_name`` for the kvlayer storage configuration will avoid
conflicts between the two systems.

A kvlayer "namespace" is a single row named ``APPNAME_NAMESPACE``.
This is a hash mapping kvlayer table names to redis row names.  A
kvlayer "table" is stored in two rows: the mapped table name with no
suffix is a hash mapping serialized UUID tuples to values, and the
mapped table name plus "k" is a sorted set of key names (only, all
with score 0, to support :meth:`RedisStorage.scan`).

This currently uses pure-ASCII key values (using
:func:`kvlayer._utils.join_key_fragments`) and intermediate table row IDs.
Redis should in principle be able to handle packed-binary values,
which would be a quarter the size.

.. _redis: http://redis.io
.. _rejester: https://github.com/diffeo/rejester

Module Contents
===============

"""

from __future__ import absolute_import

import logging
import uuid

import redis

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._exceptions import BadKey, ProgrammerError
from kvlayer._utils import join_key_fragments, split_key, make_start_key, make_end_key

logger = logging.getLogger(__name__)

# Some useful Lua fragments
verify_lua = '''
if redis.call('exists', KEYS[1]) == 0
then return redis.error_reply('no such kvlayer table') end
'''
verify_lua_failed = 'no such kvlayer table'

class RedisStorage(AbstractStorage):
    def __init__(self):
        """Initialize a redis-based storage instance.

        Uses the global kvlayer configuration, with the following parameters:

        .. code-block:: yaml

            namespace: a_namespace
        
        namespace prefix for this storage layer

        .. code-block:: yaml

            app_name: app

        application name prefix for this storage layer

        .. code-block:: yaml

            storage_addresses: ["redis.example.com:6379"]

        list of ``hostname:port`` pairs for redis (only first is used)

        .. code-block:: yaml

            redis_db_num: 1

        Redis database number (defaults to 0)

        """
        super(RedisStorage, self).__init__()
        storage_addresses = self._config.get('storage_addresses', [])
        db_num = self._config.get('redis_db_num', 0)
        if len(storage_addresses) == 0:
            raise ProgrammerError('config lacks storage_addresses')
        if len(storage_addresses) > 1:
            logger.warning('multiple storage_addresses, only first will be used')
        address = storage_addresses[0]
        if ':' in address:
            (host, port) = address.split(':')
            conn_kwargs = { 'host': host, 'port': int(port), 'db': db_num }
        else:
            conn_kwargs = { 'host': address, 'db': db_num }

        logger.debug('will connect to redis {!r}'.format(conn_kwargs))
        self._pool = redis.ConnectionPool(**conn_kwargs)
        self._table_keys = {}
        self._table_sizes = {}
        pass

    def _connection(self):
        """Get a connection to Redis."""
        return redis.StrictRedis(connection_pool=self._pool)

    @property
    def _namespace_key(self):
        """Name of the redis key holding the namespace information."""
        return self._app_name + '_' + self._namespace

    def _table_key(self, conn, table):
        """Get the redis key name of some table.

        If it is in :attr:`_table_keys` then use that value; otherwise
        look it up in redis.  Caches successful lookups.

        :param conn: Redis connection
        :type conn: :class:`redis.StrictRedis`
        :param str table: kvlayer table name
        :return: Name of the redis key holding `table`
        :rtype: str or None

        """
        if table not in self._table_keys:
            k = conn.hget(self._namespace_key, table)
            if k is not None: self._table_keys[table] = k
        return self._table_keys.get(table, None)

    def setup_namespace(self, table_names):
        """Creates tables in the namespace.

        :param table_names: Table names to create
        :type table_names: dictionary mapping string table names to
          int key lengths

        """
        conn = self._connection()
        # We will do this by generating a short random key, and
        # assigning an empty hash to it.  If we can do this successfully
        # then record the mapping.
        #
        # For simplicity we use UUIDs for the keys.  We can almost
        # certainly get away with shorter (or binary) keys.
        script = conn.register_script("""
        local existing = redis.call("hget", KEYS[2], ARGV[1])
        if existing then
          return redis.status_reply(existing)
        elseif redis.call("exists", KEYS[1]) == 0 then
          redis.call("hset", KEYS[1], "", "")
          redis.call("hset", KEYS[2], ARGV[1], KEYS[1])
          return redis.status_reply(KEYS[1])
        else
          return redis.error_reply(KEYS[1] .. " already exists")
        end
        """)
        for table in table_names.keys():
            tries = 5
            while True:
                key = uuid.uuid4().hex
                logger.debug("setup_namespace: table {} trying uuid {} in {}"
                             .format(table, key, self._namespace_key))
                try:
                    key = script(keys=[key, self._namespace_key], args=[table])
                    # will return a key name (possibly an existing key)
                    # or raise ResponseError
                    logger.debug("setup_namespace: table {} uuid {}"
                                 .format(table, key))
                    self._table_keys[table] = key
                    self._table_sizes[table] = table_names[table]
                    break
                except redis.ResponseError, exc:
                    if tries == 0:
                        raise
                    tries -= 1
                    pass # try again with a new uuid
        self.normalize_namespaces(self._table_sizes)
        return

    def delete_namespace(self):
        """Deletes all data from the namespace."""
        conn = self._connection()
        # To encourage atomicity, we will do this by
        # 1a. Get the table rows from the master row
        # 1b. Delete the master row
        # 2. Delete the invidual table rows
        script = conn.register_script("""
        local vals = redis.call('hvals', KEYS[1])
        redis.call('del', KEYS[1])
        return vals
        """)
        table_keys = script(keys=[self._namespace_key])
        if len(table_keys) > 0:
            conn.delete(*table_keys)
            conn.delete(*[k + 'k' for k in table_keys])
        self._table_keys = {}
        self._table_sizes = {}

    def clear_table(self, table_name):
        """Delete all data from one table.

        :param str table_name: Name of the kvlayer table
        :raise kvlayer._exceptions.BadKey: `table_name` does not exist
          in this namespace

        """
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(table_name)
        logger.debug('clear_table %s in namespace %s', table_name,
                     self._namespace_key)
        # There is a potential race condition the the namespace
        # is deleted *right here*.  We trap that (if the namespace
        # is deleted then the individual rows will be deleted).
        # So if you get the BadKey later on, that's why.
        script = conn.register_script(verify_lua + '''
        redis.call('del', KEYS[1])
        redis.call('del', KEYS[2])
        redis.call('hset', KEYS[1], '', '')
        ''')
        try:
            script(keys=[key, key + 'k'])
        except redis.ResponseError, exc:
            if str(exc) == verify_lua_failed:
                raise BadKey(table_name)
            raise

    def put(self, table_name, *keys_and_values, **kwargs):
        """Save values for keys in `table_name`.

        Each key must be a tuple of UUIDs for the length specified
        for `table_name` in :meth:`setup_namespace`.

        :param str table_name: Name of the kvlayer table
        :param keys_and_values: Data to add
        :type keys_and_values: pairs of (key tuple, value)
        :param int batch_size: accepted but ignored
        :raise kvlayer._exceptions.BadKey: `table_name` does not exist
          in this namespace

        """
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(table_name)
        params = []
        for (k,v) in keys_and_values:
            #logger.debug('put {} {!r} {}'.format(table_name, k, v))
            key_spec = self._table_sizes[table_name]
            ex = self.check_put_key_value(k, v, table_name,
                                          key_spec)
            if ex is not None:
                raise ex
            params.append(join_key_fragments(k, key_spec=key_spec))
            params.append(v)
        script = conn.register_script(verify_lua + '''
        for i = 1, #ARGV, 2 do
          redis.call('hset', KEYS[1], ARGV[i], ARGV[i+1])
          redis.call('zadd', KEYS[2], 0, ARGV[i])
        end
        return redis.status_reply(KEYS[1])
        ''')
        try:
            script(keys=[key, key + 'k'], args=params)
        except redis.ResponseError, exc:
            if str(exc) == verify_lua_failed:
                raise BadKey(table_name)
            raise

    def scan(self, table_name, *key_ranges, **kwargs):
        """Yield pairs from selected ranges in some table.

        `key_ranges` are pairs of tuples of :class:`uuid.UUID`,
        specifying valid lower and upper bounds for uuid-tuple keys.
        All keys in the table that are at least the lower bound of
        some range and at most the upper bound of the same range are
        returned.  If no `key_ranges` are provided, scan the entire
        table.

        >>> for (k,v) in storage.scan('table',
        ...                           ((start1,None),(end1,None)),
        ...                           ((start2,None),(end2,None))):
        ...   print k

        :param str table_name: Name of the kvlayer table
        :param key_ranges: Ranges to scan
        :type key_ranges: pairs of uuid tuples
        :raise kvlayer._exceptions.BadKey: `table_name` does not exist
          in this namespace

        """
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(table_name)
        #logger.debug('scan {} {!r}'.format(table_name, key_ranges))
        key_spec = self._table_sizes[table_name]
        if len(key_ranges) == 0:
            # scan the whole table
            # just do this in one big call for simplicity
            res = conn.hgetall(key)
            for k in sorted(res.iterkeys()):
                if k == '': continue
                uuids = split_key(k, key_spec)
                yield (uuids, res[k])
        for start, end in key_ranges:
            find_first = '''
            local first = redis.call('zrank', KEYS[2], ARGV[1])
            if not first then
              redis.call('zadd', KEYS[2], 0, ARGV[1])
              first = redis.call('zrank', KEYS[2], ARGV[1])
              redis.call('zrem', KEYS[2], ARGV[1])
            end
            '''
            no_first = '''
            local first = 0
            '''
            find_last = '''
            local last = redis.call('zrank', KEYS[2], ARGV[2])
            if not last then
              redis.call('zadd', KEYS[2], 0, ARGV[2])
              last = redis.call('zrank', KEYS[2], ARGV[2])
              redis.call('zrem', KEYS[2], ARGV[2])
              if last == 0 then return {} end
              last = last - 1
            end
            '''
            no_last = '''
            local last = -1
            '''
            do_scan = '''
            local keys = redis.call('zrange', KEYS[2], first, last)
            local result = {}
            for i = 1, #keys do
              result[i*2-1] = keys[i]
              result[i*2] = redis.call('hget', KEYS[1], keys[i])
            end
            return result
            '''
            script = verify_lua
            if start:
                script += find_first
            else:
                script += no_first
            if end:
                script += find_last
            else:
                script += no_last
            script += do_scan
            script = conn.register_script(script)
            try:
                res = script(keys=[key, key+'k'],
                             args=[make_start_key(start, key_spec=key_spec),
                                   make_end_key(end, key_spec=key_spec)])
            except redis.ResponseError, exc:
                if str(exc) == verify_lua_failed:
                    raise BadKey(table_name)
                raise
            keys = res[0::2]
            values = res[1::2]
            for k,v in zip(keys, values):
                uuids = split_key(k, key_spec)
                yield (uuids, v)

    def get(self, table_name, *keys, **kwargs):
        """Yield specific pairs from the table.

        `keys` are tuples of :class:`uuid.UUID`.  Yields pairs
        of (uuids,value) for any present values.

        >>> for (k,v) in storage.get('table', key):
        ...   assert k == key
        ...   print v

        :param str table_name: Name of the kvlayer table
        :param keys: Keys to fetch
        :type key_ranges: tuples of :class:`uuid.UUID`
        :raise kvlayer._exceptions.BadKey: `table_name` does not exist
          in this namespace

        """
        # We can be sufficiently atomic without lua scripting here.
        # The hmget call is atomic, so if the table gets deleted or
        # cleared in between checking for the table's existence
        # (which, remember, could be cached) and actually fetching keys,
        # then this yields nothing, which is consistent.
        if not keys:
            raise StopIteration
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(key)
        key_spec = self._table_sizes[table_name]
        #logger.debug('get {} {!r}'.format(table_name, keys))
        ks = [join_key_fragments(k, key_spec=key_spec) for k in keys]
        vs = conn.hmget(key, *ks)
        for (k, v) in zip(ks, vs):
            # v may be None if the key isn't there; yield it anyways
            yield (tuple(split_key(k, key_spec)),v)
            
    def delete(self, table_name, *keys, **kwargs):
        """Delete specific pairs from the table.

        `keys` are pairs of (uuids,value); the uuids are tuples of
        :class:`uuid.UUID`, and the values are ignored.

        >>> storage.delete('table', (key,None))

        :param str table_name: Name of the kvlayer table
        :param keys: Keys to delete
        :type key_ranges: pairs of (uuids,any)
        :raise kvlayer._exceptions.BadKey: `table_name` does not exist
          in this namespace

        """
        # Again blow off atomicity.  The worst that happens is that
        # the entire table is deleted in between getting its name and
        # deleting single rows from it, and in that case since the
        # entire table is deleted, all of the specific things we wanted
        # gone, are gone.
        if not keys:
            return
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(table_name)
        logger.debug('delete {} {!r}'.format(table_name, keys))
        key_spec = self._table_sizes[table_name]
        ks = [join_key_fragments(k, key_spec=key_spec) for k in keys]
        script = conn.register_script(verify_lua + '''
        for i = 1, #ARGV do
          redis.call('hdel', KEYS[1], ARGV[i])
          redis.call('zrem', KEYS[2], ARGV[i])
        end
        ''')
        try:
            script(keys=[key, key+'k'], args=ks)
        except redis.ResponseError, exc:
            if str(exc) == verify_lua_failed:
                raise BadKey(table_name)
            raise

    def close(self):
        """Close connections and end use of this storage client."""
        self._pool.disconnect()
