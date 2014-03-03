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
import logging
import re

import psycopg2

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import split_uuids, make_start_key, make_end_key, join_key_fragments
from kvlayer._exceptions import MissingID, ProgrammerError


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
  k text,
  v bytea,
  PRIMARY KEY (t, k)
);

CREATE FUNCTION upsert_{namespace}(tname TEXT, key TEXT, data BYTEA) RETURNS VOID AS
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

_DROP_TABLE = "DROP FUNCTION upsert_{namespace}(TEXT,TEXT,BYTEA)"
_DROP_TABLE_b = '''DROP TABLE kv_{namespace}'''

_CLEAR_TABLE = '''DELETE FROM kv_{namespace} WHERE t = %s'''

# use cursor.callproc() instead of SELECT query.
#_PUT = '''SELECT upsert_{namespace} (%s, %s, %s);'''

_GET = '''SELECT k, v FROM kv_{namespace} WHERE t = %s AND k = %s;'''

_GET_RANGE = '''SELECT k, v FROM kv_{namespace} WHERE t = %s AND k >= %s AND k <= %s ORDER BY k ASC;'''
_GET_RANGE_FROM_START = '''SELECT k, v FROM kv_{namespace} WHERE t = %s AND k <= %s ORDER BY k ASC;'''
_GET_RANGE_TO_END = '''SELECT k, v FROM kv_{namespace} WHERE t = %s AND k >= %s ORDER BY k ASC;'''
_GET_ALL = '''SELECT k, v FROM kv_{namespace} WHERE t = %s ORDER BY k ASC;'''

_DELETE = '''DELETE FROM kv_{namespace} WHERE t = %s AND k = %s;'''

# unused, we always _DELETE single records by key
#_DELETE_RANGE = '''DELETE FROM kv_{namespace} WHERE t = %s AND k >= %s AND K <= %s;'''


MAX_BLOB_BYTES = 15000000


def _cursor_check_namespace_table(cursor, namespace):
    cursor.execute('SELECT 1 FROM pg_tables WHERE tablename ILIKE %s', ('kv_' + namespace,))
    return cursor.rowcount > 0


class PGStorage(AbstractStorage):
    def __init__(self):
        '''Initialize a storage instance for namespace.
        uses the single string specifier for a connectionn to a postgres db
http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        '''
        super(PGStorage, self).__init__()
        if not _valid_namespace(self._namespace):
            raise ProgrammerError('namespace must match re: %r' % (_psql_identifier_re.pattern,))
        self.storage_addresses = self._config['storage_addresses']
        if not self.storage_addresses:
            raise ProgrammerError('postgres kvlayer needs config["storage_addresses"]')
        self.connection = None

    def _conn(self):
        '''internal lazy connector'''
        if self.connection is None:
            self.connection = psycopg2.connect(self.storage_addresses[0])
            self.connection.autocommit = True
        return self.connection

    def _namespace_table_exists(self):
        conn = self._conn()
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
        conn = self._conn()
        with conn.cursor() as cursor:
            if _cursor_check_namespace_table(cursor, self._namespace):
                # already exists
                logging.debug('namespace %r already exists, not creating', self._namespace)
                return
            cursor.execute(_CREATE_TABLE.format(namespace=self._namespace))

    def delete_namespace(self):
        '''Deletes all data from namespace.'''
        conn = self._conn()
        with conn.cursor() as cursor:
            if not _cursor_check_namespace_table(cursor, self._namespace):
                logging.debug('namespace %r does not exist, not dropping', self._namespace)
                return
            try:
                cursor.execute(_DROP_TABLE.format(namespace=self._namespace))
                cursor.execute(_DROP_TABLE_b.format(namespace=self._namespace))
            except:
                logging.warn('error on delete_namespace(%r)', self._namespace, exc_info=True)

    def clear_table(self, table_name):
        'Delete all data from one table'
        conn = self._conn()
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
        num_uuids = self._table_names[table_name]
        conn = self._conn()
        with conn.cursor() as cursor:
            for kv in keys_and_values:
                ex = self.check_put_key_value(kv[0], kv[1], table_name, num_uuids)
                if ex:
                    raise ex
                cursor.callproc(
                    'upsert_{namespace}'.format(namespace=self._namespace),
                    (table_name, join_key_fragments(kv[0], uuid_mode=self._require_uuid), psycopg2.Binary(kv[1])))

    def get(self, table_name, *keys, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with specified keys.
        '''
        num_uuids = self._table_names[table_name]
        cmd = _GET.format(namespace=self._namespace)
        conn = self._conn()
        with conn.cursor() as cursor:
            for key in keys:
                key = join_key_fragments(key, uuid_mode=self._require_uuid)
                cursor.execute(cmd, (table_name, key))
                if not (cursor.rowcount > 0):
                    raise MissingID()
                results = cursor.fetchmany()
                while results:
                    for row in results:
                        val = row[1]
                        if isinstance(val, buffer):
                            if len(val) > MAX_BLOB_BYTES:
                                logging.error('key=%r has blob of size %r over limit of %r', row[0], len(val), MAX_BLOB_BYTES)
                                continue  # TODO: raise instead of drop?
                            val = val[:]
                        yield tuple(split_uuids(row[0])), val
                    results = cursor.fetchmany()



    def scan(self, table_name, *key_ranges, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys within the specified ranges.  If no key_ranges
        are provided, then yield all (key, value) pairs in table.

        :type key_ranges: (((UUID, ...), (UUID, ...)), ...)
                            ^^^^^^^^^^^^^^^^^^^^^^^^
                            start        finish of one range
        '''
        num_uuids = self._table_names[table_name]
        failOnEmptyResult = True
        if not key_ranges:
            key_ranges = [['', '']]
            failOnEmptyResult = False
        def _pgkeyrange(kr):
            return (table_name, kmin, kmax)
        conn = self._conn()
        with conn.cursor() as cursor:
            for kmin, kmax in key_ranges:
                if kmin:
                    start = make_start_key(kmin, uuid_mode=self._require_uuid, num_uuids=num_uuids)
                else:
                    start = None
                if kmax:
                    finish = make_end_key(kmax, uuid_mode=self._require_uuid, num_uuids=num_uuids)
                else:
                    finish = None
                logging.debug('pg t=%r %r<=k<=%r (%r<=k<=%r)', table_name, kmin, kmax, start, finish)
                if start:
                    if finish:
                        cmd = _GET_RANGE.format(namespace=self._namespace)
                        cursor.execute(cmd, (table_name, start, finish))
                    else:
                        cmd = _GET_RANGE_TO_END.format(namespace=self._namespace)
                        cursor.execute(cmd, (table_name, start))
                else:
                    if finish:
                        cmd = _GET_RANGE_FROM_START.format(namespace=self._namespace)
                        cursor.execute(cmd, (table_name, finish))
                    else:
                        cmd = _GET_ALL.format(namespace=self._namespace)
                        cursor.execute(cmd, (table_name,))
                logging.debug('%r rows from %r', cursor.rowcount, cmd)
                if not (cursor.rowcount > 0):
                    continue
                results = cursor.fetchmany()
                while results:
                    for row in results:
                        val = row[1]
                        if isinstance(val, buffer):
                            if len(val) > MAX_BLOB_BYTES:
                                logging.error('key=%r has blob of size %r over limit of %r', row[0], len(val), MAX_BLOB_BYTES)
                                continue  # TODO: raise instead of drop?
                            val = val[:]
                        yield split_uuids(row[0]), val
                        failOnEmptyResult = False
                    results = cursor.fetchmany()
        if failOnEmptyResult:
            raise MissingID()

    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        :params batch_size: a DB-specific parameter that limits the
        number of (key, value) paris gathered into each batch for
        communication with DB.
        '''
        num_uuids = self._table_names[table_name]
        def _delkey(k):
            if len(k) != num_uuids:
                raise Exception('invalid key has %s uuids but wanted %s: %r' % (len(k), num_uuids, k))
            return (table_name, join_key_fragments(k, uuid_mode=self._require_uuid))
        conn = self._conn()
        with conn.cursor() as cursor:
            cursor.executemany(
                _DELETE.format(namespace=self._namespace),
                map(_delkey, keys))


    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self.connection:
            self.connection.close()
            self.connection = None


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
