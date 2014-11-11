'''Table-per-table kvlayer MySQL backend.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

At a high level this is very similar to :mod:`kvlayer._postgrest`.

'''
# There's a lot of overlap with kvlayer._postgrest.
# There may be room for factoring out commonalities,
# but connect and put() are pretty different.

from __future__ import absolute_import
import contextlib
import logging
import os
import re
import uuid

import mysql.connector

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._exceptions import ConfigurationError, ProgrammerError

logger = logging.getLogger(__name__)

class MysqlTableStorage(AbstractStorage):
    '''MySQL kvlayer backend.'''
    config_name = 'mysql'
    default_config = {
        'pool_size': 5,
    }
    @classmethod
    def discover_config(cls, config, prefix):
        if 'storage_addresses' in config:
            return
        ## TODO: is there equivalent docker environment for mysql?
        # addr = os.environ.get('POSTGRES_PORT_5432_TCP_ADDR')
        # port = os.environ.get('POSTGRES_PORT_5432_TCP_PORT')
        # if addr and port:
        #     config['storage_addresses'] = [addr + ':' + port]
        #     # This is what the standard Docker postgres:9.3 container gives
        #     config.setdefault('username', 'postgres')
        #     config.setdefault('password', 'postgres')  # not actually needed
        #     config.setdefault('dbname', 'postgres')

    def __init__(self, *args, **kwargs):
        '''Create a new MySQL kvlayer client.

        Config takes fields for MySQL connect function:
        ('user', 'password', 'database', 'host', 'port', 'pool_size')
        '''
        super(MysqlTableStorage, self).__init__(*args, **kwargs)
        
        # Is the namespace string valid?
        # http://dev.mysql.com/doc/refman/5.5/en/identifiers.html
        # has the real rules.
        if not re.match('[a-z_][a-z0-9_$]*', self._app_name, re.IGNORECASE):
            raise ConfigurationError('app_name {!r} must be a valid SQL name'
                                     .format(self._app_name))
        if not re.match('[a-z_][a-z0-9_$]*', self._namespace, re.IGNORECASE):
            raise ConfigurationError('namespace {!r} must be a valid SQL name'
                                     .format(self._namespace))

        # Figure out what we're connecting to
        storage_addresses = self._config.get('storage_addresses', [])
        if not storage_addresses:
            raise ConfigurationError('no storage_addresses for mysql')
        if len(storage_addresses) > 1:
            logger.warning('multiple storage_addresses for mysql, '
                           'using first only')
        #connect_string = storage_addresses[0]

        self.connect_args = {
            'autocommit': True,
        }
        for cname in ('user', 'password', 'database', 'host', 'port', 'pool_size'):
            if cname in self._config:
                self.connect_args[cname] = self._config[cname]
        if 'host' not in self.connect_args:
            self.connect_args['host'] = storage_addresses[0]

    @contextlib.contextmanager
    def _conn(self):
        '''Produce a MySQL connection from the implicit pool.

        This also runs a single transaction on that connection.  On
        successful completion, the transaction is committed; if any
        exception is thrown, the transaction is aborted.
        '''
        conn = mysql.connector.connect(**self.connect_args)
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def _cursor(self, name=None):
        '''Produce a cursor from a connection.

        This is a helper for the common case of wanting a single
        cursor on a single connection to do a single operation.

        '''
        if name is not None:
            logger.warn('stop using _cursor(name) %s', name)
        with self._conn() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def _table_name(self, table_name):
        '''Get the SQL table name of a kvlayer table.'''
        return '{}_{}.{}'.format(self._app_name, self._namespace, table_name)

    def _columns(self, key_spec):
        '''Get the names of the columns for a specific table.'''
        return ['k{}'.format(n+1) for n in xrange(len(key_spec))]

    def _massage_key_part(self, typ, kp):
        '''Change a single key part if needed.'''
        #if typ == str:
        #    return psycopg2.Binary(kp)
        if isinstance(kp, uuid.UUID):
            return kp.bytes
        return kp

    def _massage_key_tuple(self, key_spec, key):
        '''Change types in `key` to things that can be passed into sql.'''
        return tuple(self._massage_key_part(typ, kp)
                     for typ, kp in zip(key_spec, key))

    def _massage_result_part(self, typ, rp):
        '''Change a single result part if needed.'''
        #if typ == str:
        #    return rp[:]
        if typ == uuid.UUID:
            return uuid.UUID(bytes=str(rp))
        return rp

    def _massage_result_tuple(self, key_spec, row):
        '''Change types in `row` from postgres return types.'''
        return tuple(self._massage_result_part(typ, rp)
                     for typ, rp in zip(key_spec, row))

    def _python_to_sql_type(self, typ):
        if typ is int:
            return 'INTEGER'
        if typ is long:
            # We'd think "BIGINT" (64-bit) would be enough for
            # practical uses, but Python long is unbounded.
            #return 'NUMERIC(1000,0)'
            return 'NUMERIC(65)'  # MySQL limit
        if typ is uuid.UUID:
            return 'BINARY(16)'
        if typ is str: # but not unicode; bytes in Python 3
            # MEDIUMBLOB was not allowed to be in the PRIMARY KEY.
            # I didn't manage to find the docs on what _can_ be in a PRIMARY KEY
            # but this works and is hopefully good enough for our data?
            # The total size of key is limited to either 767 bytes or 3072 bytes.
            # http://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html
            return 'VARBINARY(255)'  # up to 2^24 bytes
        if isinstance(typ, tuple):
            # Other backends just pass typ as the second argument to
            # isinstance() and don't use the information to serialize.
            # Right now we can handle this iff the types are all
            # "similar", particularly integers.
            typs = [(long, int)]
            for candidates in typs:
                if all(t in candidates for t in typ):
                    return self._python_to_sql_type(candidates[0])
        raise ProgrammerError('unexpected key type {!r}'.format(typ))

    def setup_namespace(self, table_names):
        '''Create tables in the namespace.

        One table is created in MySQL for each table in
        `table_names`.  As elsewhere in :mod:`kvlayer`, if a table's
        value is an integer, it is interpreted as a tuple of that many
        :class:`uuid.UUID` objects.  Otherwise, this accepts tuples
        of :func:`int`, :func:`long`, :class:`uuid.UUID`, and
        :func:`str`; these are mapped to the SQL types ``INTEGER``,
        ``BIGINT``, ``BINARY(16)``, and ``VARBINARY(255)``, repsectively.

        '''
        super(MysqlTableStorage, self).setup_namespace(table_names)
        with self._cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS {}_{}'
                           .format(self._app_name, self._namespace))
        for name, key_spec in self._table_names.iteritems():
            with self._cursor() as cursor:
                cnames = self._columns(key_spec)
                ctypes = [self._python_to_sql_type(t) for t in key_spec]
                columns = ['{} {} NOT NULL'.format(n, t)
                           for n, t in zip(cnames, ctypes)]
                sql = ('CREATE TABLE IF NOT EXISTS {} ({}, v MEDIUMBLOB NOT NULL, '
                       'PRIMARY KEY ({}))'
                       .format(self._table_name(name), ', '.join(columns),
                               ', '.join(cnames)))
                logger.debug(sql)
                cursor.execute(sql)

    def delete_namespace(self):
        '''Find and delete all of the tables.'''
        with self._cursor() as cursor:
            cursor.execute('DROP SCHEMA IF EXISTS {}_{}'
                           .format(self._app_name, self._namespace))

    def clear_table(self, table_name):
        '''Clear out a single table.'''
        with self._cursor() as cursor:
            cursor.execute('TRUNCATE {}'.format(self._table_name(table_name)))

    def put(self, table_name, *keys_and_values, **kwargs):
        '''Write data into a table.'''
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        colstr = ', '.join(cnames)
        valqs = ', '.join(['%s'] * (len(cnames) + 1))
        with self._cursor() as cursor:
            for k, v in keys_and_values:
                self.check_put_key_value(k, v, table_name, key_spec)
                k = self._massage_key_tuple(key_spec, k)
                v = self._massage_key_part(str, v)
                # cursor.callproc(self._table_name('upsert_' + table_name),
                #                 k + (v,))
                sql = 'INSERT INTO {tn} ({colstr}, v) VALUES ({valqs}) ON DUPLICATE KEY UPDATE v=%s'.format(
                    tn=tn,
                    colstr=colstr,
                    valqs=valqs
                )
                key = self._massage_key_tuple(key_spec, k)
                sqargs = key + (v, v)
                logger.debug('put %r %r', sql, sqargs)
                cursor.execute(sql, sqargs)

    def get(self, table_name, *keys, **kwargs):
        '''Get values out of the database.'''
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        exprs = ['{}=%s'.format(kn) for kn in cnames]
        where = 'WHERE {}'.format(' AND '.join(exprs))
        sql = 'SELECT v FROM ' + tn + ' ' + where
        with self._cursor() as cursor:
            for k in keys:
                pg_key = self._massage_key_tuple(key_spec, k)
                cursor.execute(sql, pg_key)
                found = False
                for row in cursor:
                    assert not found
                    found = True
                    yield k, self._massage_result_part(str, row[0])
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
        query = ' AND '.join(['{}{}%s'.format(cn, op) for (cn, kp) in z])
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
                query = '({}) OR ({}) OR ({})'.format(q1, q2, q3)
            else:
                query = '({}) OR ({})'.format(q1, q2)
        else:
            if q3:
                query = '({}) OR ({})'.format(q2, q3)
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
            where = ' OR '.join(['({})'.format(p[0]) for p in parts])
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
        cnames = self._columns(key_spec)
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {}'.format(', '.join(cnames))
        with self._cursor(name='scan') as cursor:
            sql = ('SELECT {}, v FROM {} {} {}'
                   .format(', '.join(cnames), tn, where, order))
            cursor.execute(sql, wt)
            for row in cursor:
                k = row[:-1]
                v = row[-1]
                yield (self._massage_result_tuple(key_spec, k),
                       self._massage_result_part(str, v))

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        '''Get ordered key tuples out of the database.'''
        # Just like scan().
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {}'.format(', '.join(cnames))
        with self._cursor(name='scan') as cursor:
            cursor.execute('SELECT {} FROM {} {} {}'
                           .format(', '.join(cnames), tn, where, order),
                           wt)
            for row in cursor:
                yield self._massage_result_tuple(key_spec, row)

    def delete(self, table_name, *keys, **kwargs):
        tn = self._table_name(table_name)
        key_spec = self._table_names[table_name]
        cnames = self._columns(key_spec)
        exprs = ['{}=%s'.format(kn) for kn in cnames]
        where = 'WHERE {}'.format(' AND '.join(exprs))
        sql = 'DELETE FROM ' + tn + ' ' + where
        with self._cursor() as cursor:
            for k in keys:
                k = self._massage_key_tuple(key_spec, k)
                cursor.execute(sql, k)

    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
            finally:
                self.connection_pool = None
