'''
Implementation of AbstractStorage using Postgres

Requires that you have the psycopg2 module installed in your environment:

  easy_install psycopg2
OR
  pip install psycopg2

This software is released under an MIT/X11 open source license.

Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import contextlib
import logging
import re

import psycopg2
import psycopg2.pool

from kvlayer._abstract_storage import StringKeyedStorage
from kvlayer._exceptions import ProgrammerError


logger = logging.getLogger(__name__)


# SQL strings in this module use python3 style string.format() formatting
# to substitute the table name into the command.


# this is not precisely right.
# http://www.postgresql.org/docs/9.3/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
# super-ascii 'letter' chars are also allowed.
# Perhaps in the all-unicode-all-the-time Python3 re package there will be
# better support for characters classes needed to specify this right.
_psql_identifier_re = re.compile(r'[a-z_][a-z0-9_$]*', re.IGNORECASE)


def _valid_namespace(x):
    return bool(_psql_identifier_re.match(x))


# kv_{namespace}
# table, key, value
_CREATE_TABLE = '''CREATE TABLE kv_{namespace} (
  t text,
  k bytea,
  v bytea,
  PRIMARY KEY (t, k)
);

CREATE FUNCTION upsert_{namespace}(tname TEXT, key BYTEA, data BYTEA)
  RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE kv_{namespace} SET v = data WHERE t = tname AND k = key;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO kv_{namespace}(t,k,v) VALUES (tname, key, data);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
'''

_DROP_TABLE = "DROP FUNCTION upsert_{namespace}(TEXT,BYTEA,BYTEA)"
_DROP_TABLE_b = '''DROP TABLE kv_{namespace}'''

_CLEAR_TABLE = '''DELETE FROM kv_{namespace} WHERE t = %s'''

# use cursor.callproc() instead of SELECT query.
# _PUT = '''SELECT upsert_{namespace} (%s, %s, %s);'''

_GET_KV = 'SELECT k, v FROM kv_{namespace} WHERE t=%s'
_GET_K = 'SELECT k FROM kv_{namespace} WHERE t=%s'

_GET_EXACT = ' AND k=%s'
_GET_MIN = ' AND k>=%s'
_GET_MAX = ' AND k<%s'
_SCAN_ORDER = ' ORDER BY k ASC'
_INNER_LIMIT = ' LIMIT %s'

_GET = _GET_KV + _GET_EXACT

_DELETE = '''DELETE FROM kv_{namespace} WHERE t = %s AND k = %s;'''

# unused, we always _DELETE single records by key
# _DELETE_RANGE = ('DELETE FROM kv_{namespace} WHERE t = %s AND k >= %s '
#                  'AND K <= %s;''')

# TODO: use this query to list available namespaces
# select tablename from pg_catalog.pg_tables where tablename like 'kv_%';


MAX_BLOB_BYTES = 15000000


def _cursor_check_namespace_table(cursor, namespace):
    cursor.execute('SELECT 1 FROM pg_tables WHERE tablename ILIKE %s',
                   ('kv_' + namespace,))
    return cursor.rowcount > 0


class PGStorage(StringKeyedStorage):
    def __init__(self, *args, **kwargs):
        '''Initialize a storage instance for namespace.
        uses the single string specifier for a connectionn to a postgres db
http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        '''
        super(PGStorage, self).__init__(*args, **kwargs)
        if not _valid_namespace(self._namespace):
            raise ProgrammerError('namespace must match re: %r' %
                                  (_psql_identifier_re.pattern,))
        self.storage_addresses = self._config['storage_addresses']
        if not self.storage_addresses:
            raise ProgrammerError(
                'postgres kvlayer needs config["storage_addresses"]')
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            self._config.get('min_connections', 2),
            self._config.get('max_connections', 16),
            self.storage_addresses[0]
        )
        self._scan_inner_limit = int(self._config.get('scan_inner_limit',
                                                      1000))

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
        conn = self.connection_pool.getconn()
        try:
            with conn:
                yield conn
        finally:
            # This has logic to test whether the connection is closed
            # and/or failed and correctly manages returning it to the
            # pool (or not).
            self.connection_pool.putconn(conn)

    def _namespace_table_exists(self):
        with self._conn() as conn:
            with conn.cursor() as cursor:
                return _cursor_check_namespace_table(cursor, self._namespace)

    def setup_namespace(self, table_names, value_types={}):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.

        :param table_names: Each string in table_names becomes the
        name of a table, and the value must be an integer specifying
        the number of UUIDs in the keys

        :type table_names: dict(str = int)
        '''
        super(PGStorage, self).setup_namespace(table_names, value_types)
        with self._conn() as conn:
            with conn.cursor() as cursor:
                if _cursor_check_namespace_table(cursor, self._namespace):
                    # already exists
                    logger.debug('namespace %r already exists, not creating',
                                 self._namespace)
                    return
                cursor.execute(_CREATE_TABLE.format(namespace=self._namespace))

    def delete_namespace(self):
        '''Deletes all data from namespace.'''
        with self._conn() as conn:
            with conn.cursor() as cursor:
                if not _cursor_check_namespace_table(cursor, self._namespace):
                    logger.debug('namespace %r does not exist, not dropping',
                                 self._namespace)
                    return
                try:
                    cursor.execute(
                        _DROP_TABLE.format(namespace=self._namespace))
                    cursor.execute(
                        _DROP_TABLE_b.format(namespace=self._namespace))
                except:
                    logger.warn('error on delete_namespace(%r)',
                                self._namespace, exc_info=True)

    def clear_table(self, table_name):
        'Delete all data from one table'
        with self._conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    _CLEAR_TABLE.format(namespace=self._namespace),
                    (table_name,)
                )

    def _put(self, table_name, keys_and_values):
        with self._conn() as conn:
            with conn.cursor() as cursor:
                for (k, v) in keys_and_values:
                    cursor.callproc(
                        'upsert_{namespace}'.format(namespace=self._namespace),
                        (table_name, psycopg2.Binary(k), psycopg2.Binary(v)))

    def _unmarshal_k(self, row, key_spec):
        '''Get the key tuple from a response row.'''
        keyraw = row[0]
        if isinstance(keyraw, buffer):
            keyraw = keyraw[:]
        return self._encoder.deserialize(keyraw, key_spec)

    def _unmarshal_kv(self, row, key_spec):
        '''Get the (key,value) pair from a response row.'''
        val = row[1]
        if isinstance(val, buffer):
            if len(val) > MAX_BLOB_BYTES:
                logger.error('key=%r has blob of size %r over limit of %r',
                             row[0], len(val), MAX_BLOB_BYTES)
                return None
            val = val[:]
        key = self._unmarshal_k(row, key_spec)
        return (key, val)

    def _get(self, table_name, keys):
        cmd = _GET.format(namespace=self._namespace)
        with self._conn() as conn:
            for key in keys:
                with conn.cursor(name='get') as cursor:
                    cursor.execute(cmd, (table_name, psycopg2.Binary(key)))
                    found = False
                    for row in cursor:
                        k = row[0]
                        if k is not None:
                            k = k[:]
                        v = row[1]
                        if v is not None:
                            v = v[:]  # un-bufferify
                            found = True
                            yield (k, v)
                    if not found:
                        yield (key, None)

    def _scan(self, table_name, key_ranges):
        for kmin, kmax in (key_ranges or [['', '']]):
            for rkey, rval in self._scan_subscan_kminmax(
                    table_name, kmin, kmax):
                yield rkey, rval

    def _scan_keys(self, table_name, key_ranges):
        for kmin, kmax in (key_ranges or [['', '']]):
            for rkey in self._scan_subscan_kminmax(
                    table_name, kmin, kmax, with_values=False):
                    yield rkey

    def _scan_subscan_kminmax(self, table_name, kmin, kmax, with_values=True):
        prevkey = None
        while True:
            count = 0
            for p in self._scan_kminmax(table_name, kmin, kmax, with_values):
                if with_values:
                    rkey = p[0]
                else:
                    rkey = p
                count += 1
                if rkey != prevkey:
                    # don't double-return an edge value
                    yield p
                prevkey = rkey
            if not self._scan_inner_limit or count < self._scan_inner_limit:
                # we didn't get up to limit, we must be done
                return
            # else, we hit limit, we need to scan for more
            kmin = rkey

    def _scan_kminmax(self, table_name, kmin, kmax, with_values=True):
        if with_values:
            query = _GET_KV
        else:
            query = _GET_K
        query = query.format(namespace=self._namespace)
        args = [table_name]
        if kmin:
            query += _GET_MIN
            args.append(psycopg2.Binary(kmin))
        if kmax:
            query += _GET_MAX
            args.append(psycopg2.Binary(kmax))
        query += _SCAN_ORDER
        if self._scan_inner_limit:
            query += _INNER_LIMIT
            args.append(self._scan_inner_limit)
        with self._conn() as conn:
            with conn.cursor(name='scan') as cursor:
                cursor.execute(query, tuple(args))
                for row in cursor:
                    k = row[0]
                    if k is not None:
                        k = k[:]  # unbufferify
                    if with_values:
                        v = row[1]
                        if v is not None:
                            v = v[:]
                        yield (k, v)
                    else:
                        yield k

    def _delete(self, table_name, keys):
        with self._conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    _DELETE.format(namespace=self._namespace),
                    [(table_name, psycopg2.Binary(k)) for k in keys])

    # don't mark this one detatch_on_exception, that would be silly
    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
            finally:
                self.connection_pool = None


# run this to cleanup any cruft from kvlayer unit tests
CLEAN_TESTS = '''
CREATE OR REPLACE FUNCTION clean_tests() RETURNS VOID AS
$$
DECLARE
  argtypes text;
  tat text;
  toid oid;
  pnargs pg_proc%ROWTYPE;
  pt pg_tables%ROWTYPE;
BEGIN
  FOR pnargs IN SELECT * from pg_proc where proname like '%upsert_test_%' LOOP
    SELECT typname FROM pg_type WHERE oid = pnargs.proargtypes[0]
      INTO argtypes;
    FOR i in 1..array_upper(pnargs.proargtypes,1) LOOP
      SELECT typname FROM pg_type WHERE oid = pnargs.proargtypes[i] INTO tat;
      argtypes := argtypes || ',' || tat;
    END LOOP;
    EXECUTE 'DROP FUNCTION ' || pnargs.proname || '(' || argtypes || ');';
  END LOOP;
  FOR pt IN SELECT * FROM pg_tables WHERE tablename LIKE '%kv_test_%' LOOP
    EXECUTE 'DROP TABLE ' || pt.tablename || ';';
  END LOOP;
END
$$ LANGUAGE plpgsql;


SELECT clean_tests();
'''
