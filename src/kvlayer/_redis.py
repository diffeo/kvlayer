"""Redis kvlayer storage implementation.

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
kvlayer "table" is a hash stored in a single row, where the keys of
the hash are UUID tuples.

This currently uses pure-ASCII key values (using
:func:`kvlayer._utils.join_uuids`) and intermediate table row IDs.
Redis should in principle be able to handle packed-binary values,
which would be a quarter the size.

-----

Your use of this software is governed by your license agreement.

Copyright 2014 Diffeo, Inc.

.. _redis: http://redis.io
.. _rejester: https://github.com/diffeo/rejester

"""

from __future__ import absolute_import

import logging
import uuid

import redis

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._exceptions import BadKey, MissingID, ProgrammerError
from kvlayer._utils import join_uuids, split_uuids

logger = logging.getLogger(__name__)

class RedisStorage(AbstractStorage):
    def __init__(self, config):
        """Initialize a redis-based storage instance.

        `config` is a dictionary that may include the following
        configuration parameters:

        ``namespace``
          namespace prefix for this storage layer
        ``app_name``
          application name prefix for this storage layer
        ``storage_addresses``
          list of ``hostname:port`` pairs for redis (only first is used)

        """
        super(RedisStorage, self).__init__(config)
        storage_addresses = config.get('storage_addresses', [])
        if len(storage_addresses) == 0:
            raise ProgrammerError('config lacks storage_addresses')
        if len(storage_addresses) > 1:
            logger.warning('multiple storage_addresses, only first will be used')
        address = storage_addresses[0]
        if ':' in address:
            (host,port) = address.split(':')
            conn_kwargs = { 'host': host, 'port': int(port) }
        else:
            conn_kwargs = { 'host': address }

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
            if k is not none: self._table_keys[table] = k
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
            raise BadKey(key)
        # There is a potential race condition the the namespace
        # is deleted *right here*.  We trap that (if the namespace
        # is deleted then the individual rows will be deleted).
        # So if you get the BadKey later on, that's why.
        script = conn.register_script("""
        if redis.call('exists', KEYS[1]) == 0
        then return redis.error_reply(KEYS[1]) end
        redis.call('del', KEYS[1])
        redis.call('hset', KEYS[1], '', '')
        return redis.status_reply(KEYS[1])
        """)
        ret = script(keys=[key])
        if 'err' in ret:
            raise BadKey(ret['err'])

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
            raise BadKey(key)
        params = []
        for (k,v) in keys_and_values:
            logger.debug('put {} {!r} {}'.format(table_name, k, v))
            ex = self.check_put_key_value(k, v, table_name,
                                          self._table_sizes[table_name])
            if ex is not None:
                raise ex
            params.append(join_uuids(*k))
            params.append(v)
        script = conn.register_script("""
        if redis.call('exists', KEYS[1]) == 0
        then return redis.error_reply(KEYS[1]) end
        for i = 1, #ARGV, 2 do
          redis.call('hset', KEYS[1], ARGV[i], ARGV[i+1])
        end
        return redis.status_reply(KEYS[1])
        """)
        ret = script(keys=[key], args=params)
        if 'err' in ret:
            raise BadKey(ret['err'])

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
        :raise kvlayer._exceptions.MissingID: at least one key range was
          specified but no items were returned

        """
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(key)
        logger.debug('scan {} {!r}'.format(table_name, key_ranges))
        # It doesn't look like redis has any sort of useful range
        # query on any data type, except for the (non-unique) "score"
        # side of sorted sets.  Even then finding start/end elements
        # of a range isn't entirely trivial if they don't exist
        # (we could do a bisection search).  This also requires
        # creating a secondary index.
        #
        # Until that works, let's just get a dump of everything in
        # the hash and hope there's not too much there.
        num_uuids = self._table_sizes[table_name]
        ranges = [(join_uuids(*l, num_uuids=num_uuids, padding='0'),
                   join_uuids(*u, num_uuids=num_uuids, padding='f'))
                  for (l,u) in key_ranges]
        def valid(k):
            if k == '': return False # placeholder
            if len(ranges) == 0: return True
            for (l,u) in ranges:
                if k >= l and k <= u: return True
            return False
        res = conn.hgetall(key)
        found = False
        for (k,v) in res.iteritems():
            if valid(k):
                found = True
                uuids = split_uuids(k)
                yield (uuids,v)
        if len(ranges) > 0 and not found:
            raise MissingID(key_ranges)

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
        :raise kvlayer._exceptions.MissingID: none of `keys` exist in
          this table

        """
        # We can be sufficiently atomic without lua scripting here.
        # The hmget call is atomic, so if the table gets deleted or
        # cleared in between checking for the table's existence
        # (which, remember, could be cached) and actually fetching keys,
        # then this yields nothing, which is consistent.
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(key)
        logger.debug('get {} {!r}'.format(table_name, keys))
        ks = [join_uuids(*k) for k in keys]
        vs = conn.hmget(key, *ks)
        found = False
        for (k,v) in zip(ks,vs):
            if v is not None:
                found = True
                yield (tuple(split_uuids(k)),v)
        if not found:
            raise MissingID(keys)
            
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
        conn = self._connection()
        key = self._table_key(conn, table_name)
        if key is None:
            raise BadKey(key)
        logger.debug('delete {} {!r}'.format(table_name, keys))
        ks = [join_uuids(*k) for k in keys]
        conn.hdel(key, *ks)

    def close(self):
        """Close connections and end use of this storage client."""
        self._pool.disconnect()
