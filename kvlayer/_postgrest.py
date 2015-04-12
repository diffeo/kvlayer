'''Table-per-table kvlayer PostgreSQL backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

At a high level this is very similar to :mod:`kvlayer._postgres`.
However, that implementation puts all kvlayer data into a single SQL
table with a single string key.  If there is a large amount of data
this table can become very large, and the unstructured string keys
limit the database's abilities to do some kinds of searches.

Since the underlying SQL tables look totally different, this isn't
compatible with the older implementation, and since all of the queries
are totally different, it makes more sense to just do a separate
implementation.

'''
# Also, there's a lot of overlap with kvlayer._postgres.  I expect that
# this will *replace* that implementation and maybe become a base for
# a future generic-SQL backend, so I'm not going to be embarrassed by
# the copy-and-paste.
#
# A brief word on "upsert": kvlayer semantics follow generic Bigtable
# semantics, that a put() call inserts keys if they do not exist and
# replaces keys if they do.  This is a long-contentious point in
# PostgreSQL.  There is a reference single-row stored procedure at
# http://www.postgresql.org/docs/9.3/static/plpgsql-control-structures.html
# but this has a couple of practical problems for kvlayer: put() is
# frequently called with multiple rows at once and the stored procedure
# all but forces a network round-trip per row; and there is a startup
# race condition with the stored procedure.  But, for this case, see also
# http://stackoverflow.com/q/1109061/398670
#
# For multi-row upsert, a pattern that works in many places is to
# create a temporary table, populate it, then do the merge.
# http://stackoverflow.com/questions/17267417/how-do-i-do-an-upsert-merge-insert-on-duplicate-update-in-postgresql
# has a good example of doing this safely in PostgreSQL.  In practice
# at least one minimal benchmark that does do multiple-value put()
# does quite a bit better with this implementation.

from __future__ import absolute_import
import contextlib
import logging
import os
import re
import time
import uuid

import psycopg2
import psycopg2.errorcodes
import psycopg2.extras
import psycopg2.pool

from kvlayer._abstract_storage import AbstractStorage, ACCUMULATOR, COUNTER
from kvlayer._exceptions import ConfigurationError, ProgrammerError

logger = logging.getLogger(__name__)
psycopg2.extras.register_uuid()


class PostgresTableStorage(AbstractStorage):
    '''PostgreSQL kvlayer backend.

    This implementation uses one SQL table per kvlayer table, inside an
    SQL schema per kvlayer namespace.

    :meth:`increment` is atomic in this backend for both
    :class:`~kvlayer._abstract_storage.COUNTER` and
    :class:`~kvlayer._abstract_storage.ACCUMULATOR` types.

    '''
    config_name = 'postgrest'
    default_config = {
        'min_connections': 2,
        'max_connections': 16,
    }

    @classmethod
    def discover_config(cls, config, prefix):
        if 'storage_addresses' in config:
            return
        addr = os.environ.get('POSTGRES_PORT_5432_TCP_ADDR')
        port = os.environ.get('POSTGRES_PORT_5432_TCP_PORT')
        if addr and port:
            config['storage_addresses'] = [addr + ':' + port]
            # This is what the standard Docker postgres:9.3 container gives
            config.setdefault('username', 'postgres')
            config.setdefault('password', 'postgres')  # not actually needed
            config.setdefault('dbname', 'postgres')

    def __init__(self, *args, **kwargs):
        '''Create a new PostgreSQL kvlayer client.'''
        super(PostgresTableStorage, self).__init__(*args, **kwargs)

        # Is the namespace string valid?
        # http://www.postgresql.org/docs/9.3/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        # has the real rules.
        if not re.match('[a-z_][a-z0-9_$]*', self._app_name, re.IGNORECASE):
            raise ConfigurationError('app_name {0!r} must be a valid SQL name'
                                     .format(self._app_name))
        if not re.match('[a-z_][a-z0-9_$]*', self._namespace, re.IGNORECASE):
            raise ConfigurationError('namespace {0!r} must be a valid SQL name'
                                     .format(self._namespace))

        # Figure out what we're connecting to
        storage_addresses = self._config.get('storage_addresses', [])
        if not storage_addresses:
            raise ConfigurationError('no storage_addresses for postgrest')
        if len(storage_addresses) > 1:
            logger.warning('multiple storage_addresses for postgrest, '
                           'using first only')
        connect_string = storage_addresses[0]
        # This is cheap, but a multi-part k=v string and a host[:port]
        # string should be pretty easy to tell apart
        if '=' not in connect_string and ' ' not in connect_string:
            if ':' in connect_string:
                (host, port) = connect_string.split(':', 2)
                connect_string = 'host={0} port={1}'.format(host, port)
            else:
                connect_string = 'host={0}'.format(connect_string)
            user = self._config.get('username', None)
            if not user:
                raise ConfigurationError('no username for postgrest')
            password = self._config.get('password', None)
            if not password:
                raise ConfigurationError('no password for postgrest')
            dbname = self._config.get('dbname', None)
            if not dbname:
                raise ConfigurationError('no dbname for postgrest')
            connect_string += ' user={0} password={1} dbname={2}'.format(
                user, password, dbname)
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            self._config.get('min_connections', 2),
            self._config.get('max_connections', 16),
            connect_string
        )

    @contextlib.contextmanager
    def _conn(self):
        '''Produce a PostgreSQL connection from the pool.

        This also runs a single transaction on that connection.  On
        successful completion, the transaction is committed; if any
        exception is thrown, the transaction is aborted.

        On successful completion the connection is returned to the
        pool for reuse.  If any exception is thrown, the connection
        is closed.

        '''
        tries = 5
        for _ in xrange(tries):
            conn = self.connection_pool.getconn()
            try:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute('SELECT 1')
                except (psycopg2.DatabaseError, psycopg2.InterfaceError):
                    logging.warn('connection is gone, maybe retrying...',
                                 exc_info=True)
                    time.sleep(0.5)
                    continue
                with conn:
                    yield conn
                    break
            finally:
                # This has logic to test whether the connection is closed
                # and/or failed and correctly manages returning it to the
                # pool (or not).
                self.connection_pool.putconn(conn)

    @contextlib.contextmanager
    def _cursor(self, name=None):
        '''Produce a cursor from a connection.

        This is a helper for the common case of wanting a single
        cursor on a single connection to do a single operation.

        '''
        with self._conn() as conn:
            with conn.cursor(name=name) as cursor:
                yield cursor

    def _table_name(self, table_name):
        '''Get the SQL table name of a kvlayer table.'''
        return '{0}_{1}.{2}'.format(self._app_name, self._namespace, table_name)

    def _columns(self, key_spec):
        '''Get the names of the columns for a specific table.'''
        return ['k{0}'.format(n+1) for n in xrange(len(key_spec))]

    def _massage_key_part(self, typ, kp):
        '''Change a single key part if needed.'''
        if typ == str:
            return psycopg2.Binary(kp)
        return kp

    def _massage_key_tuple(self, key_spec, key):
        '''Change types in `key` to things that can be passed into postgres.'''
        return tuple(self._massage_key_part(typ, kp)
                     for typ, kp in zip(key_spec, key))

    def _massage_result_part(self, typ, rp):
        '''Change a single result part if needed.'''
        if typ == str:
            return rp[:]
        return rp

    def _massage_result_tuple(self, key_spec, row):
        '''Change types in `row` from postgres return types.'''
        return tuple(self._massage_result_part(typ, rp)
                     for typ, rp in zip(key_spec, row))

    def _python_to_sql_type(self, typ):
        if typ is int or typ == COUNTER:
            return 'INTEGER'
        if typ is long:
            # We'd think "BIGINT" (64-bit) would be enough for
            # practical uses, but Python long is unbounded.
            return 'NUMERIC(1000,0)'
        if typ is uuid.UUID:
            return 'UUID'
        if typ is str:  # but not unicode; bytes in Python 3
            return 'BYTEA'
        if typ is float or typ == ACCUMULATOR:
            return 'DOUBLE PRECISION'
        if isinstance(typ, tuple):
            # Other backends just pass typ as the second argument to
            # isinstance() and don't use the information to serialize.
            # Right now we can handle this iff the types are all
            # "similar", particularly integers.
            typs = [(long, int)]
            for candidates in typs:
                if all(t in candidates for t in typ):
                    return self._python_to_sql_type(candidates[0])
        raise ProgrammerError('unexpected key type {0!r}'.format(typ))

    def setup_namespace(self, table_names, value_types={}):
        '''Create tables in the namespace.

        One table is created in PostgreSQL for each table in
        `table_names`.  As elsewhere in :mod:`kvlayer`, if a table's
        value is an integer, it is interpreted as a tuple of that many
        :class:`uuid.UUID` objects.  Otherwise, this accepts tuples
        of :func:`int`, :func:`long`, :class:`uuid.UUID`, and
        :func:`str`; these are mapped to the SQL types ``INTEGER``,
        ``BIGINT``, ``UUID``, and ``BYTEA``, repsectively.  (The last
        two are PostgreSQL extensions.)

        '''
        super(PostgresTableStorage, self).setup_namespace(
            table_names, value_types)
        with self._cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS {0}_{1}'
                           .format(self._app_name, self._namespace))
        for name, key_spec in self._table_names.iteritems():
            with self._cursor() as cursor:
                cnames = self._columns(key_spec)
                ctypes = [self._python_to_sql_type(t) for t in key_spec]
                vtype = self._python_to_sql_type(self._value_types[name])
                columns = ['{0} {1} NOT NULL'.format(n, t)
                           for n, t in zip(cnames, ctypes)]
                sql = ('CREATE TABLE IF NOT EXISTS {0} ({1}, v {2} NOT NULL, '
                       'PRIMARY KEY ({3}))'
                       .format(self._table_name(name), ', '.join(columns),
                               vtype, ', '.join(cnames)))
                cursor.execute(sql)

    def delete_namespace(self):
        '''Find and delete all of the tables.'''
        with self._cursor() as cursor:
            cursor.execute('DROP SCHEMA IF EXISTS {0}_{1} CASCADE'
                           .format(self._app_name, self._namespace))

    def clear_table(self, table_name):
        '''Clear out a single table.'''
        with self._cursor() as cursor:
            cursor.execute('TRUNCATE {0}'.format(self._table_name(table_name)))

    def put(self, table_name, *keys_and_values, **kwargs):
        '''Write data into a table.'''
        if not keys_and_values:
            return
        key_spec = self._table_names[table_name]
        value_type = self._value_types[table_name]
        tn = self._table_name(table_name)
        cnames = self._columns(key_spec)
        cname = cnames[0]
        keys_equal = ' AND '.join('{0}.{1}=put.{1}'.format(tn, col)
                                  for col in cnames)
        keys_value = ', '.join('put.{0}'.format(c) for c in cnames + ['v'])
        if kwargs.get('is_increment', False):
            new_value = '{0}.v+put.v'.format(tn)
        else:
            new_value = 'put.v'
        for k, v in keys_and_values:
            self.check_put_key_value(k, v, table_name, key_spec)
        kvps = [self._massage_key_tuple(key_spec, k) +
                (self._massage_key_part(value_type, v),)
                for (k, v) in keys_and_values]
        with self._cursor() as cursor:
            # DANGER WILL ROBINSON: we are manually constructing the
            # query parameters, but at least we're relying on the
            # library to do escaping for us
            template = '(' + ','.join(['%s'] * (len(key_spec) + 1)) + ')'
            values = ','.join(cursor.mogrify(template, row)
                              for row in kvps)
            # Now we get to do one massive multi-statement query
            q = ('CREATE TEMPORARY TABLE put (LIKE {tn}) ON COMMIT DROP; '
                 'INSERT INTO put VALUES {values}; '
                 'LOCK TABLE {tn} IN EXCLUSIVE MODE; '
                 'UPDATE {tn} SET v={new_value} FROM put WHERE {keys_equal}; '
                 'INSERT INTO {tn}'
                 ' SELECT {keys_value}'
                 ' FROM put LEFT OUTER JOIN {tn} ON ({keys_equal})'
                 ' WHERE {tn}.{cname} IS NULL').format(
                     tn=tn, values=values, new_value=new_value,
                     keys_equal=keys_equal, template=template,
                     keys_value=keys_value, cname=cname)
            cursor.execute(q)

    def get(self, table_name, *keys, **kwargs):
        '''Get values out of the database.'''
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        value_type = self._value_types[table_name]
        cnames = self._columns(key_spec)
        exprs = ['{0}=%s'.format(kn) for kn in cnames]
        where = 'WHERE {0}'.format(' AND '.join(exprs))
        sql = 'SELECT v FROM ' + tn + ' ' + where
        with self._cursor() as cursor:
            for k in keys:
                pg_key = self._massage_key_tuple(key_spec, k)
                cursor.execute(sql, pg_key)
                found = False
                for row in cursor:
                    assert not found
                    found = True
                    yield k, self._massage_result_part(value_type, row[0])
                if not found:
                    yield k, None

    def _scan_padded(self, key_spec, k):
        '''Add :const:`None` to the end of `k` so it's the right length'''
        if k is None:
            k = ()
        k += (None,) * (len(k) - len(key_spec))
        return k

    def _scan_where_one(self, cnames, key_spec, op, key):
        '''``tuple(kN) `op` key`` as part of a WHERE clause'''
        z = zip(cnames, self._massage_key_tuple(key_spec, key))
        query = ' AND '.join(['{0}{1}%s'.format(cn, op) for (cn, kp) in z])
        wt = tuple(kp for (cn, kp) in z)
        return (query, wt)

    def _scan_where_parts(self, cnames, key_spec, lo, hi):
        '''``tuple(kN) >= pair[0] AND tuple(kN) <= pair[1]``'''
        # Simple, expected cases:
        # (None,(anything)) ==> tuple(kN) <= anything
        # ((anything),None) ==> tuple(kN) >= anything
        # ((k1val,),(kp1val,)) ==> k1 == k1val (ignoring k2, k3, ...)
        # ((k1lo,),(k1hi,)) ==> k1 >= k1lo AND k1 <= k1hi
        # ((k1val,k2lo),(k1val,)) ==> k1=k1val and k2>=k2lo
        # ((k1val,k2lo),(k1val,k2hi)) ==> k1=k1val AND k2>=k2lo AND k2<=k2hi
        #
        # Harder cases:
        # ((a,b),(c,d)) breaks down into
        #  k1=a and k2>=b
        #  k1>a and k1<c
        #  k1=c and k2<=d
        # ((a,b),(c,) breaks down into
        #  k1=a and k2>=b
        #  k1>a and k1<=c
        if len(lo) == 0 and len(hi) == 0:
            return ('', ())
        if len(hi) == 0:
            return self._scan_where_one(cnames, key_spec, '>=', lo)
        if len(lo) == 0:
            return self._scan_where_one(cnames, key_spec, '<=', hi)
        if lo[0] == hi[0]:
            (query, params) = self._scan_where_parts(
                cnames[1:], key_spec[1:], lo[1:], hi[1:])
            query = cnames[0] + '=%s' + (query and ' AND ' + query)
            return (query,
                    (self._massage_key_part(key_spec[0], lo[0]),) + params)
        assert lo[0] < hi[0]
        if len(lo) == 1 and len(hi) == 1:
            return (cnames[0] + '>=%s AND ' + cnames[0] + '<=%s',
                    (self._massage_key_part(key_spec[0], lo[0]),
                     self._massage_key_part(key_spec[0], hi[0])))
        # Otherwise: the two keys differ in their first part, and
        # this isn't a trivial scan.
        if len(lo) == 1:
            # (a,) to (c,d)
            # So, (k1>=a AND k1<c) OR (k1=c and k2<d)
            #     (a,) to (<c,)    OR (c,) to (c,d)
            (query, params) = self._scan_where_parts(
                cnames, key_spec, hi[:1], hi)
            my_query = cnames[0] + '>=%s AND ' + cnames[0] + '<%s'
            if query:
                query = '(' + my_query + ') OR (' + query + ')'
            else:
                query = my_query
            params = (self._massage_key_part(key_spec[0], lo[0]),
                      self._massage_key_part(key_spec[0], hi[0])) + params
            return (query, params)
        if len(hi) == 1:
            # (a,b) to (c,)
            # So, (k1=a AND k2>=b) OR (k1>a AND k1<=c)
            #     (a,b) to (a,)    OR (>a,) to (c,)
            (query, params) = self._scan_where_parts(
                cnames, key_spec, lo, lo[:1])
            my_query = cnames[0] + '>%s AND ' + cnames[0] + '<=%s'
            if query:
                query = '(' + query + ') OR (' + my_query + ')'
            else:
                query = my_query
            params = params + (self._massage_key_part(key_spec[0], lo[0]),
                               self._massage_key_part(key_spec[0], hi[0]))
            return (query, params)
        # (a,b) to (c,d)
        # This is a combination of all of the above:
        # (k1=a AND k2>=b) OR (k1>a AND k1<c) OR (k1=c AND k2<=d)
        (q1, p1) = self._scan_where_parts(
            cnames, key_spec, lo, lo[:1]) # (a,b) to (a,)
        q2 = cnames[0] + '>%s AND ' + cnames[0] + '<%s'
        p2 = (self._massage_key_part(key_spec[0], lo[0]),
              self._massage_key_part(key_spec[0], hi[0]))
        (q3, p3) = self._scan_where_parts(
            cnames, key_spec, hi[:1], hi) # (c,) to (c,d)
        if q1:
            if q3:
                query = '({0}) OR ({1}) OR ({2})'.format(q1, q2, q3)
            else:
                query = '({0}) OR ({1})'.format(q1, q2)
        else:
            if q3:
                query = '({0}) OR ({1})'.format(q2, q3)
            else:
                query = q2
        params = p1 + p2 + p3
        return (query, params)

    def _scan_where(self, cnames, key_spec, key_ranges):
        '''Build an SQL WHERE clause for a scan.

        Returns a pair of the actual WHERE clause and the values that need
        to be passed into it.'''
        if len(key_ranges) == 0 or (None, None) in key_ranges:
            return '', () # just scan the whole table
        parts = [self._scan_where_parts(cnames, key_spec,
                                        list(lo or []), list(hi or []))
                 for (lo, hi) in key_ranges]
        parts = [(query, params) for (query, params) in parts if query]
        where = ''
        if parts:
            where = ' OR '.join(['({0})'.format(p[0]) for p in parts])
            where = 'WHERE ' + where
        wt = sum([p[1] for p in parts], ())
        return (where, wt)

    def scan(self, table_name, *key_ranges, **kwargs):
        '''Get ordered (key, value) pairs out of the database.'''
        # We have previously asserted that there is a significant cost
        # to running a very large scan in a transaction attributable to
        # keeping corresponding row versions around.  See the
        # scan_inner_limit parameter in _postgres.py and its
        # implementation.
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        value_type = self._value_types[table_name]
        cnames = self._columns(key_spec)
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {0}'.format(', '.join(cnames))
        with self._cursor(name='scan') as cursor:
            sql = ('SELECT {0}, v FROM {1} {2} {3}'
                   .format(', '.join(cnames), tn, where, order))
            cursor.execute(sql, wt)
            for row in cursor:
                k = row[:-1]
                v = row[-1]
                yield (self._massage_result_tuple(key_spec, k),
                       self._massage_result_part(value_type, v))

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        '''Get ordered key tuples out of the database.'''
        # Just like scan().
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {0}'.format(', '.join(cnames))
        with self._cursor(name='scan') as cursor:
            cursor.execute('SELECT {0} FROM {1} {2} {3}'
                           .format(', '.join(cnames), tn, where, order),
                           wt)
            for row in cursor:
                yield self._massage_result_tuple(key_spec, row)

    def delete(self, table_name, *keys, **kwargs):
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        exprs = ['{0}=%s'.format(kn) for kn in cnames]
        where = 'WHERE {0}'.format(' AND '.join(exprs))
        sql = 'DELETE FROM ' + tn + ' ' + where
        with self._cursor() as cursor:
            for k in keys:
                k = self._massage_key_tuple(key_spec, k)
                cursor.execute(sql, k)

    def increment(self, table_name, *keys_and_values):
        if self._value_types[table_name] not in [COUNTER, ACCUMULATOR]:
            raise ProgrammerError('table {0} is not a counter table'
                                  .format(table_name))
        self.put(table_name, *keys_and_values, is_increment=True)

    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
            finally:
                self.connection_pool = None
