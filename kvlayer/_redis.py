"""Redis kvlayer storage implementation.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

This is an implementation of :mod:`kvlayer` that uses redis_ for its
underlying storage.  This generally limits storage to what can be held
in memory on the host system, but at the same time, redis is expected
to be available on many systems and requires much less setup than a
distributed Bigtable system.

Redis is also used by rejester_ for its underlying storage.  Using
a different ``app_name`` for the kvlayer storage configuration will avoid
conflicts between the two systems.

A kvlayer "namespace" is a single row named ``APPNAME_NAMESPACE``.
This is a hash mapping kvlayer table names to redis row names.  A
kvlayer "table" is stored in two rows: the mapped table name with no
suffix is a hash mapping serialized UUID tuples to values, and the
mapped table name plus "k" is a sorted set of key names (only, all
with score 0, to support :meth:`RedisStorage.scan`).

.. _redis: http://redis.io
.. _rejester: https://github.com/diffeo/rejester

.. autoclass:: RedisStorage

"""

from __future__ import absolute_import

import logging
import uuid

import redis

from kvlayer._abstract_storage import StringKeyedStorage
from kvlayer._exceptions import BadKey, ProgrammerError

logger = logging.getLogger(__name__)

# Some useful Lua fragments
verify_lua = '''
if redis.call('exists', KEYS[1]) == 0
then return redis.error_reply('no such kvlayer table') end
'''
verify_lua_failed = 'no such kvlayer table'


class RedisStorage(StringKeyedStorage):
    def __init__(self, *args, **kwargs):
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
        super(RedisStorage, self).__init__(*args, **kwargs)
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

        logger.debug('will connect to redis {0!r}'.format(conn_kwargs))
        self._pool = redis.ConnectionPool(**conn_kwargs)
        self._table_keys = {}
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
            if k is not None:
                self._table_keys[table] = k
        return self._table_keys.get(table, None)

    def setup_namespace(self, table_names, value_types={}):
        """Creates tables in the namespace.

        :param table_names: Table names to create
        :type table_names: dictionary mapping string table names to
          int key lengths

        """
        super(RedisStorage, self).setup_namespace(table_names, value_types)
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
                logger.debug("setup_namespace: table {0} trying uuid {1} in {2}"
                             .format(table, key, self._namespace_key))
                try:
                    key = script(keys=[key, self._namespace_key], args=[table])
                    # will return a key name (possibly an existing key)
                    # or raise ResponseError
                    logger.debug("setup_namespace: table {0} uuid {1}"
                                 .format(table, key))
                    self._table_keys[table] = key
                    break
                except redis.ResponseError, exc:
                    if tries == 0:
                        raise
                    tries -= 1
                    pass # try again with a new uuid
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

    def _put(self, table_name, keys_and_values):
        conn = self._connection()
        table_key = self._table_key(conn, table_name)
        if table_key is None:
            raise BadKey(table_name)
        table_key_k = table_key + 'k'

        pipeline = conn.pipeline(transaction=False)
        for (k, v) in keys_and_values:
            pipeline.hset(table_key, k, v)
            pipeline.zadd(table_key_k, 0, k)
        pipeline.execute()

    def _scan(self, table_name, key_ranges):
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(table_name)
        if len(key_ranges) == 0:
            # scan the whole table
            # just do this in one big call for simplicity
            res = conn.hgetall(key)
            return [(k, res[k]) for k in sorted(res.iterkeys()) if k != '']
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
                res = script(keys=[key, key+'k'], args=[start, end])
            except redis.ResponseError, exc:
                if str(exc) == verify_lua_failed:
                    raise BadKey(table_name)
                raise
            keys = res[0::2]
            values = res[1::2]
            return zip(keys, values)

    def _get(self, table_name, keys):
        # We can be sufficiently atomic without lua scripting here.
        # The hmget call is atomic, so if the table gets deleted or
        # cleared in between checking for the table's existence
        # (which, remember, could be cached) and actually fetching keys,
        # then this yields nothing, which is consistent.
        if not keys:
            return []
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(key)
        values = conn.hmget(key, *keys)
        # values may include None if the key isn't there; return it anyways
        return zip(keys, values)

    def _delete(self, table_name, keys):
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
        script = conn.register_script(verify_lua + '''
        for i = 1, #ARGV do
          redis.call('hdel', KEYS[1], ARGV[i])
          redis.call('zrem', KEYS[2], ARGV[i])
        end
        ''')
        try:
            script(keys=[key, key+'k'], args=keys)
        except redis.ResponseError, exc:
            if str(exc) == verify_lua_failed:
                raise BadKey(table_name)
            raise

    def close(self):
        """Close connections and end use of this storage client."""
        self._pool.disconnect()
