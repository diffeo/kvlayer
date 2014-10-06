'''Implementation of AbstractStorage using MongoDB

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''

from __future__ import absolute_import
import logging
import random

#from bson.objectid import ObjectId
from bson.binary import Binary
import pymongo

from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import make_start_key, make_end_key, \
    serialize_key, deserialize_key

logger = logging.getLogger(__name__)

class MongoStorage(AbstractStorage):
    '''
    configure with:
    storage_addresses: ['connection string', ...]
    database: database name or leave empty for database='{app_name}_{namespace}'
    '''

    def __init__(self, *args, **kwargs):
        super(MongoStorage, self).__init__(*args, **kwargs)

        self._storage_addresses = self._config.get('storage_addresses', [])
        self._database_name = self._config.get('database', None)
        if self._database_name is None:
            if self._app_name:
                if self._namespace:
                    self._database_name = self._app_name + '_' + self._namespace
                else:
                    self._database_name = self._app_name
            else:
                assert self._namespace
                self._database_name = self._namespace
        self._connection = None

    def _conn(self):
        if self._connection is None:
            self._connection = pymongo.MongoClient(random.choice(self._storage_addresses))
        return self._connection

    def _db(self):
        return self._conn()[self._database_name]

    def setup_namespace(self, table_names):
        super(MongoStorage, self).setup_namespace(table_names)
        # So schemaless. Very nosql.

    def delete_namespace(self):
        self._conn().drop_database(self._database_name)

    def clear_table(self, table_name):
        self._db().drop_collection(table_name)

    def put(self, table_name, *keys_and_values, **kwargs):
        key_spec = self._table_names[table_name]
        table = self._db()[table_name]
        #def putiter():
        for k,v in keys_and_values:
            self.check_put_key_value(k, v, table_name, key_spec)
            joined_key = Binary(serialize_key(k, key_spec=key_spec))
            table.update({'_id': joined_key}, {'_id': joined_key, 'v': Binary(v)}, upsert=True)
            #yield {'_id': joined_key, 'v': v}

        #self._db()[table_name].insert(putiter())

    def scan(self, table_name, *key_ranges, **kwargs):
        key_spec = self._table_names[table_name]
        table = self._db()[table_name]
        if not key_ranges:
            key_ranges = [['', '']]
        for start_key, stop_key in key_ranges:
            rangeq = {}
            if start_key:
                rangeq['$gte'] = Binary(make_start_key(start_key, key_spec=key_spec))
            if stop_key:
                rangeq['$lt'] = Binary(make_end_key(stop_key, key_spec=key_spec))
            if rangeq:
                logger.debug('scan range %r', rangeq)
                rowiter = table.find({'_id': rangeq})
            else:
                rowiter = table.find()
            for row in rowiter:
                key = deserialize_key(row['_id'], key_spec)
                yield key, _demunge(row['v'])

    # TODO: is there a way to make mongo smarter and only return keys?
    #def scan_keys(self, table_name, *key_ranges, **kwargs):

    def get(self, table_name, *keys, **kwargs):
        key_spec = self._table_names[table_name]
        table = self._db()[table_name]
        for k in keys:
            row = table.find_one({'_id': Binary(serialize_key(k, key_spec=key_spec))})
            if not row:
                yield (k, None)
            else:
                yield (k, _demunge(row['v']))

    def delete(self, table_name, *keys, **kwargs):
        key_spec = self._table_names[table_name]
        table = self._db()[table_name]
        for k in keys:
            table.remove(Binary(serialize_key(k, key_spec=key_spec)))

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def _demunge(x):
    if isinstance(x, Binary):
        return str(x)
    return x
