'''
Implementation of AbstractStorage using Postgres

Requires that you have the psycopg2 module installed in your environment:

  easy_install psycopg2
OR
  pip install psycopg2

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import contextlib
import logging
import re
import time

import psycopg2
import psycopg2.pool

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import split_key, make_start_key, make_end_key, join_key_fragments
from kvlayer._exceptions import ProgrammerError


logger = logging.getLogger(__name__)


# SQL strings in this module use python3 style string.format() formatting to substitute the table name into the command.


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

CREATE FUNCTION upsert_{namespace}(tname TEXT, key BYTEA, data BYTEA) RETURNS VOID AS
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
#_PUT = '''SELECT upsert_{namespace} (%s, %s, %s);'''

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
#_DELETE_RANGE = '''DELETE FROM kv_{namespace} WHERE t = %s AND k >= %s AND K <= %s;'''

# TODO: use this query to list available namespaces
# select tablename from pg_catalog.pg_tables where tablename like 'kv_%';


MAX_BLOB_BYTES = 15000000


def _cursor_check_namespace_table(cursor, namespace):
    cursor.execute('SELECT 1 FROM pg_tables WHERE tablename ILIKE %s', ('kv_' + namespace,))
    return cursor.rowcount > 0

class PGStorage(AbstractStorage):
    def __init__(self, *args, **kwargs):
        '''Initialize a storage instance for namespace.
        uses the single string specifier for a connectionn to a postgres db
http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        '''
        super(PGStorage, self).__init__(*args, **kwargs)
        if not _valid_namespace(self._namespace):
            raise ProgrammerError('namespace must match re: %r' % (_psql_identifier_re.pattern,))
        self.storage_addresses = self._config['storage_addresses']
        if not self.storage_addresses:
            raise ProgrammerError('postgres kvlayer needs config["storage_addresses"]')
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            self._config.get('min_connections', 2),
            self._config.get('max_connections', 16),
            self.storage_addresses[0]
        )
        self._scan_inner_limit = int(self._config.get('scan_inner_limit', 1000))

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

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.

        :param table_names: Each string in table_names becomes the
        name of a table, and the value must be an integer specifying
        the number of UUIDs in the keys

        :type table_names: dict(str = int)
        '''
        self._table_names.update(table_names)
        self.normalize_namespaces(self._table_names)
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

    def put(self, table_name, *keys_and_values, **kwargs):
        '''Save values for keys in table_name.  Each key must be a
        tuple of UUIDs of the length specified for table_name in
        setup_namespace.

        :params batch_size: a DB-specific parameter that limits the
        number of (key, value) paris gathered into each batch for
        communication with DB.
        '''
        start_time = time.time()
        keys_size = 0
        values_size = 0
        num_keys = 0

        key_spec = self._table_names[table_name]
        with self._conn() as conn:
            with conn.cursor() as cursor:
                for kv in keys_and_values:
                    self.check_put_key_value(kv[0], kv[1], table_name,
                                             key_spec)
                    num_keys += 1
                    keystr = join_key_fragments(kv[0], key_spec=key_spec)
                    keys_size += len(keystr)
                    values_size += len(kv[1])
                    #logger.debug('put k=%r from %r', keystr, kv[0])
                    keystr = psycopg2.Binary(keystr)
                    cursor.callproc(
                        'upsert_{namespace}'.format(namespace=self._namespace),
                        (table_name, keystr, psycopg2.Binary(kv[1])))

        end_time = time.time()
        num_values = num_keys

        self.log_put(table_name, start_time, end_time, num_keys, keys_size,
                     num_values, values_size)

    def _unmarshal_k(self, row, key_spec):
        '''Get the key tuple from a response row.'''
        keyraw = row[0]
        if isinstance(keyraw, buffer):
            keyraw = keyraw[:]
        return split_key(keyraw, key_spec)

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

    def get(self, table_name, *keys, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with specified keys.
        '''
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        num_values = 0
        values_size = 0

        key_spec = self._table_names[table_name]
        cmd = _GET.format(namespace=self._namespace)
        try:
            with self._conn() as conn:
                for key in keys:
                    num_keys += 1
                    bkey = join_key_fragments(key, key_spec=key_spec)
                    keys_size += len(bkey)
                    bkey = psycopg2.Binary(bkey)
                    with conn.cursor(name='get') as cursor:
                        cursor.execute(cmd, (table_name, bkey))
                        found = False
                        for row in cursor:
                            p = self._unmarshal_kv(row, key_spec)
                            if p is None:
                                continue
                            num_values += 1
                            values_size += len(p[1])
                            yield p
                            found = True
                        if not found:
                            yield key, None
        finally:
            end_time = time.time()
            self.log_get(table_name, start_time, end_time, num_keys,
                         keys_size, num_values, values_size)

    def scan(self, table_name, *key_ranges, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys within the specified ranges.  If no key_ranges
        are provided, then yield all (key, value) pairs in table.

        :type key_ranges: (((UUID, ...), (UUID, ...)), ...)
                            ^^^^^^^^^^^^^^^^^^^^^^^^
                            start        finish of one range
        '''
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        values_size = 0
        key_spec = self._table_names[table_name]

        try:
            for kmin, kmax in (key_ranges or [['', '']]):
                for rkey, rval in self._scan_subscan_kminmax(key_spec, table_name,
                                                             kmin, kmax):
                    yield rkey, rval

                    num_keys += 1
                    keys_size += sum(len(str(kp)) for kp in rkey)
                    values_size += len(rval)

        finally:
            end_time = time.time()
            num_values = num_keys
            self.log_scan(table_name, start_time, end_time, num_keys,
                          keys_size, num_values, values_size)

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        '''Scan only the keys from a table.

        Yield key tuples from querying table_name for items with keys
        within the specified ranges.  If no key_ranges are provided,
        then yield all keys in table.

        This is equivalent to::

            itertools.imap(lambda (k,v): k,
                           self.scan(table_name, *key_ranges, **kwargs))

        But it avoids copying the (potentially large) data values across
        the network.

        :param str table_name: name of table to scan
        :param key_ranges: (`start`,`end`) key range pairs

        '''
        start_time = time.time()
        num_keys = 0
        keys_size = 0
        key_spec = self._table_names[table_name]

        try:
            for kmin, kmax in (key_ranges or [['', '']]):
                for rkey in self._scan_subscan_kminmax(key_spec, table_name,
                                                       kmin, kmax,
                                                       with_values=False):
                    yield rkey

                    num_keys += 1
                    keys_size += sum(len(str(kp)) for kp in rkey)

        finally:
            end_time = time.time()
            self.log_scan_keys(table_name, start_time, end_time, num_keys, keys_size)

    def _scan_subscan_kminmax(self, key_spec, table_name, kmin, kmax,
                              with_values=True):
        prevkey = None
        while True:
            count = 0
            for p in self._scan_kminmax(key_spec, table_name, kmin, kmax,
                                        with_values):
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

    def _scan_kminmax(self, key_spec, table_name, kmin, kmax, with_values=True):
        if kmin:
            start = make_start_key(kmin, key_spec=key_spec)
            start = psycopg2.Binary(start)
        else:
            start = None
        if kmax:
            finish = make_end_key(kmax, key_spec=key_spec)
            finish = psycopg2.Binary(finish)
        else:
            finish = None
        #logger.debug('pg t=%r %r<=k<=%r (%s<=k<=%s)', table_name, kmin, kmax, start, finish)
        if with_values:
            query = _GET_KV
            unmarshal = lambda row: self._unmarshal_kv(row, key_spec)
        else:
            query = _GET_K
            unmarshal = lambda row: self._unmarshal_k(row, key_spec)
        query = query.format(namespace=self._namespace)
        args = [table_name]
        if start:
            query += _GET_MIN
            args.append(start)
        if finish:
            query += _GET_MAX
            args.append(finish)
        query += _SCAN_ORDER
        if self._scan_inner_limit:
            query += _INNER_LIMIT
            args.append(self._scan_inner_limit)
        with self._conn() as conn:
            with conn.cursor(name='scan') as cursor:
                cursor.execute(query, tuple(args))
                for row in cursor:
                    yield unmarshal(row)

    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        :params batch_size: a DB-specific parameter that limits the
        number of (key, value) paris gathered into each batch for
        communication with DB.
        '''
        start_time = time.time()
        num_keys = 0
        keys_size = 0

        key_spec = self._table_names[table_name]
        delete_statement_args = []
        for k in keys:
            if len(k) != len(key_spec):
                raise Exception('invalid key has %s uuids but wanted %s: %r' % (len(k), len(key_spec), k))
            joined_key = join_key_fragments(k, key_spec=key_spec)
            num_keys += 1
            keys_size += len(joined_key)
            delete_statement_args.append(
                (table_name, psycopg2.Binary(joined_key))
            )

        with self._conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    _DELETE.format(namespace=self._namespace),
                    delete_statement_args)

        end_time = time.time()
        self.log_delete(table_name, start_time, end_time, num_keys, keys_size)

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
    SELECT typname FROM pg_type WHERE oid = pnargs.proargtypes[0] INTO argtypes;
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
