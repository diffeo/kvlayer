'''kvlayer backend for Riak.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

'''
from __future__ import absolute_import

import riak

from kvlayer._abstract_storage import StringKeyedStorage


class RiakStorage(StringKeyedStorage):
    def __init__(self, *args, **kwargs):
        '''Create a new Riak client object.

        This sets up a :class:`riak.RiakStorage` according to configuration,
        but in general this will not result in a network connection.

        '''
        super(RiakStorage, self).__init__(*args, **kwargs)

        def make_node(s):
            if isinstance(s, basestring):
                return {'host': s}
            return s
        nodes = [make_node(s) for s in self._config['storage_addresses']]
        self.connection = riak.RiakClient(
            protocol=self._config.get('protocol', 'pbc'),
            nodes=nodes)
        self.scan_limit = self._config.get('scan_limit', 100)

    def _bucket(self, table):
        '''Riak bucket name for a kvlayer table.'''
        name = '{}_{}_{}'.format(self._app_name, self._namespace, table)
        return self.connection.bucket(name)

    def delete_namespace(self):
        '''Deletes all data from the namespace.

        This only actually deletes keys in known namespaces, as per
        :meth:`setup_namespace`.  It needs to iterate and individually
        delete every single key.

        '''
        for table in self._table_names.iterkeys():
            self.clear_table(table)

    def clear_table(self, table_name):
        '''Deletes all data from a single table.

        This needs to iterate and delete every single key in the
        corresponding Riak bucket.

        '''
        # See the riak-users thread around
        # http://permalink.gmane.org/gmane.comp.db.riak.user/14411
        # This strikes me as a little unsafe (shouldn't we push a vclock
        # into the delete operation?) but it's what that thread endorses
        bucket = self._bucket(table_name)
        for k in bucket.get_keys():
            bucket.delete(k)

    def _put(self, table_name, keys_and_values):
        '''Write some data to a table.

        Because of the way Riak works, each key/value pair is a separate
        write operation.  This backend makes no distinction between one
        write with multiple keys and multiple writes with one key.

        :param keys_and_values: data items to write
        :paramtype keys_and_values: pairs of (key, value)

        '''
        bucket = self._bucket(table_name)
        for k, v in keys_and_values:
            # Always do this with a read/write to maintain vector clock
            # consistency...even though this means we're pushing objects
            # around more than we need to
            obj = bucket.get(k)
            obj.encoded_data = v
            obj.content_type = 'application/octet-stream'
            obj.store()

    def _scan(self, table_name, key_ranges):
        '''Scan key/value ranges from a table.

        This is not a native Riak operation!  It is implemented as an
        index scan, over the special index ``$key``.

        '''
        return self._do_scan(table_name, key_ranges, with_values=True)

    def _scan_keys(self, table_name, key_ranges):
        '''Scan key ranges from a table.

        This is not a native Riak operation!  It is implemented as an
        index scan, over the special index ``$key``.

        '''
        return self._do_scan(table_name, key_ranges, with_values=False)

    def _do_scan(self, table_name, key_ranges, with_values=False):
        bucket = self._bucket(table_name)

        # Can this be a map/reduce job?  This would save us from the
        # requirement to keep a secondary index duplicating the key,
        # and correspondingly the requirement to use leveldb.  But,
        # the documentation notes in big letters, "Riak MapReduce is
        # intended for batch processing, not real time querying", and
        # there are some comments elsewhere in the documentation about
        # limitations of it (map jobs always query with effective R=1,
        # for instance).  In particular MR appears to always circulate
        # the key list, which makes it bad for large scans.

        if not key_ranges:
            key_ranges = [(None, None)]
        for start_key, end_key in key_ranges:
            if not start_key:
                start_key = b'\0'
            if not end_key:
                end_key = b'\xff'

            results = bucket.get_index('$key', startkey=start_key,
                                       endkey=end_key,
                                       max_results=self.scan_limit)
            while True:
                for key in results:
                    # Contrary to what the Riak documentation claims,
                    # in practice the $key and $bucket indexes seem
                    # to contain every key that ever existed.  That
                    # means we must do a fetch to ensure the key
                    # really exists.
                    obj = bucket.get(key)
                    if obj.exists:
                        if with_values:
                            yield (key, obj.encoded_data)
                        else:
                            yield key
                # NB: work around a bug in the Riak 1.4.9 protobuf
                # client.  If this is the last page of results, it
                # sets results.continuation='', but
                # IndexPage.has_next_page() tests "is None".  We
                # really want to be calling has_next_page().
                if results.continuation:  # results.has_next_page():
                    results = results.next_page()
                else:
                    break

    def _get(self, table_name, keys):
        '''Yield tuples of (key, value) for specific keys.'''
        bucket = self._bucket(table_name)

        # We can, in principle, use bucket.multiget() here.  That's
        # a complicated thing that fires up a thread pool under the
        # hood for lots of concurrent fetches.  In practice, get()
        # key lists are almost always pretty small.

        for key in keys:
            obj = bucket.get(key)
            if obj.exists:
                yield (key, obj.encoded_data)
            else:
                yield (key, None)

    def _delete(self, table_name, keys):
        '''Delete some specific keys.'''
        bucket = self._bucket(table_name)

        for key in keys:
            # Always do this with a read/write to maintain vector clock
            # consistency...even though this means we're pushing objects
            # around more than we need to
            obj = bucket.get(key)
            obj.delete()

    def close(self):
        '''End use of this storage client.

        While the Python Riak client maintains an internal connection
        pool, it is not exposed through the system API, and there is
        no obvious way to shut it down.

        '''
        super(RiakStorage, self).close()
        self.connection = None
