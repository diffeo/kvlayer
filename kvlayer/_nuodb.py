from __future__ import absolute_import
from itertools import imap
import logging
import re
import uuid

import pynuodb

from kvlayer._abstract_storage import AbstractStorage

logger = logging.getLogger(__name__)


_connection_option_names = ('database', 'host', 'user', 'password', 'options')

# e.g.
# connection_options = {'host':'localhost', 'user':'dba', 'password':'goalie', 'database': 'test'}


class NuodbStorage(AbstractStorage):
    config_name = 'nuodb'
    default_config = {
        'database': None,
        'host': None,
        'user': None,
        'password': None,
        # there may be further 'options': {}
    }

    def __init__(self, *args, **kwargs):
        super(NuodbStorage, self).__init__(*args, **kwargs)

        self._connection = None
        sparts = []
        if self._app_name:
            sparts.append(self._app_name)
        assert self._namespace
        sparts.append(self._namespace)
        self._schema = '_'.join(sparts)
        assert is_valid_symbol(self._schema)
        self._schema_exists = False

    def _conn(self):
        if self._connection is None:
            connection_options = {k:self._config.get(k) for k in _connection_option_names}
            self._connection = pynuodb.connect(**connection_options)
        return self._connection

    def _ensure_schema(self):
        if self._schema_exists:
            return
        conn = self._conn()
        cu = conn.cursor()
        cu.execute("select * from system.schemas where schema = ?;", (self._schema,))
        row = cu.fetchone()
        if row is not None:
            # schema exists. done.
            self._schema_exists = True
            cu.close()
            return

        # can't use param substitution '?', schema needs to be a bare symbol
        cu.execute("create schema {}".format(self._schema))
        self._schema_exists = True
        cu.close()

    def _create_table(self, name, key_spec):
        # create table foo (a string, b binary(16), c integer, v blob, primary key (a,b,c));
        # create table foo (a varbinary(500), b binary(16), c integer, v blob, primary key (a,b,c));

        assert is_valid_symbol(name)
        schema_table = self._schema + '.' + name
        cols = [('k' + str(i+1), _python_to_sql_type(ks)) for i, ks in enumerate(key_spec)]
        colspecs = ['{} {} NOT NULL'.format(kn, kt) for kn,kt in cols]
        sql = 'CREATE TABLE IF NOT EXISTS {} ({}, v BLOB NOT NULL, PRIMARY KEY ({}))'.format(schema_table, ', '.join(colspecs), ', '.join(map(lambda x: x[0], cols)))
        cu = self._conn().cursor()
        logger.debug(sql)
        cu.execute(sql)
        cu.close()

    def setup_namespace(self, table_names):
        super(NuodbStorage, self).setup_namespace(table_names)
        self._ensure_schema()
        cu = self._conn().cursor()
        cu.execute("SELECT TABLENAME, TABLEID FROM System.Tables where SCHEMA = ?", (self._schema,))
        existing_tables = dict(cu.fetchall())
        cu.close()
        for name, key_spec in self._table_names.iteritems():
            assert is_valid_symbol(name)
            if name in existing_tables:
                logger.debug('%s.%s exists', self._schema, name)
                continue
            self._create_table(name, key_spec)

    def delete_namespace(self):
        cu = self._conn().cursor()
        cu.execute("DROP SCHEMA {} CASCADE".format(self._schema))
        cu.close()

    def clear_table(self, table_name):
        assert is_valid_symbol(table_name)
        cu = self._conn().cursor()
        cu.execute("TRUNCATE TABLE {}.{}".format(self._schema, table_name))
        cu.close()

    def put(self, table_name, *keys_and_values, **kwargs):
        key_spec = self._table_names[table_name]
        key_column_names = ', '.join(['k{}'.format(x) for x in range(1, len(key_spec)+1)])
        qmarks = ', '.join(['?'] * len(key_spec))
        sql = "INSERT INTO {}.{} ({}, v) VALUES ({}, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)".format(self._schema, table_name, key_column_names, qmarks)
        logger.debug("nuo put: %s", sql)
        cu = self._conn().cursor()
        #cu.executemany(sql, imap(_translator(key_spec), keys_and_values))
        for kv in imap(_translator(key_spec), keys_and_values):
            cu.execute(sql, kv)
        cu.close()

    def get(self, table_name, *keys, **kwargs):
        key_spec = self._table_names[table_name]
        wheres = ' AND '.join(['WHERE k{} = ?'.format(kn) for kn in range(1, len(key_spec)+1)])
        sql = "SELECT v FROM {}.{} {}".format(self._schema, table_name, wheres)
        logger.debug('nuo get: %s', sql)
        cu = self._conn().cursor()
        for k in keys:
            cu.execute(sql, k)
            row = cu.fetchone()
            v = ((row is not None) and row[0]) or None
            yield k, v
        cu.close()
        # TODO: optimize with executemany ?
        #cu.executemany(sql, keys)

    def scan(self, table_name, *key_ranges, **kwargs):
        key_spec = self._table_names[table_name]
        cnames = ['k{}'.format(x+1) for x in xrange(len(key_spec))]
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {}'.format(', '.join(cnames))
        sql = 'SELECT {}, v FROM {}.{} {} {}'.format(
            ', '.join(cnames), self._schema, table_name, where, order)
        logger.debug('nuo scan: %s', sql)

        cu = self._conn().cursor()
        cu.execute(sql, wt)
        row = cu.fetchone()
        while row is not None:
            k = row[:-1]
            v = row[-1]
            yield (self._massage_result_tuple(key_spec, k),
                   self._massage_result_part(str, v))

            row = cu.fetchone()
        cu.close()

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        key_spec = self._table_names[table_name]
        cnames = ['k{}'.format(x+1) for x in xrange(len(key_spec))]
        (where, wt) = self._scan_where(cnames, key_spec, key_ranges)
        order = 'ORDER BY {}'.format(', '.join(cnames))
        sql = 'SELECT {} FROM {}.{} {} {}'.format(
            ', '.join(cnames), self._schema, table_name, where, order)
        logger.debug('nuo scan keys: %s', sql)

        cu = self._conn().cursor()
        cu.execute(sql, wt)
        row = cu.fetchone()
        while row is not None:
            yield self._massage_result_tuple(key_spec, row)
            row = cu.fetchone()
        cu.close()

    def delete(self, table_name, *keys, **kwargs):
        key_spec = self._table_names[table_name]
        cnames = ['k{}'.format(x+1) for x in xrange(len(key_spec))]
        exprs = ['{}=%s'.format(kn) for kn in cnames]
        where = 'WHERE {}'.format(' AND '.join(exprs))
        sql = 'DELETE FROM ' + self._schema + '.' + table_name + ' ' + where
        logger.debug('nuo delete: %s', sql)

        cu = self._conn().cursor()
        cu.executemany(sql, imap(lambda x: self._massage_key_tuple(key_spec, x), keys))
        cu.close()

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _massage_key_tuple(self, key_spec, key):
        return key

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



# _translator + _translate_kv is somewhat overwrought, but if we need
# to do any type conversion this is the place.
def _translate_kv(key_spec, key_value):
    key, value = key_value
    assert len(key) == len(key_spec)
    ob = key + (value,)
    logger.debug('kv %r', ob)
    return ob

def _translator(key_spec):
    return lambda x: _translate_kv(key_spec, x)


_VALID_SCHEMA_RE = re.compile(r'^[a-zA-Z]+[a-zA-Z0-9_]*$')


def is_valid_symbol(x):
    return bool(_VALID_SCHEMA_RE.match(x))


# similar to postgrest.
def _python_to_sql_type(typ):
    if typ is int:
        return 'INTEGER'
    if typ is long:
        # We'd think "BIGINT" (64-bit) would be enough for
        # practical uses, but Python long is unbounded.
        return 'NUMERIC(1000,0)'
    if typ is uuid.UUID:
        return 'BINARY(16)'
    if typ is str: # but not unicode; bytes in Python 3
        return 'VARBINARY(500)'
    if isinstance(typ, tuple):
        # Other backends just pass typ as the second argument to
        # isinstance() and don't use the information to serialize.
        # Right now we can handle this iff the types are all
        # "similar", particularly integers.
        typs = [(long, int)]
        for candidates in typs:
            if all(t in candidates for t in typ):
                return _python_to_sql_type(candidates[0])
    raise ProgrammerError('unexpected key type {!r}'.format(typ))
