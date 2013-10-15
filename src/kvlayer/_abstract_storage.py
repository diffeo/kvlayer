'''
Definition of AbstractStorage, which all storage
implementations inherit.

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import abc


class AbstractStorage(object):
    '''
    base class for all low-level storage implementations

    All of the table-like structures we use are setup like this:
        namespace = dict(
            table_name = dict((UUID, UUID, ...): val)
            ...
        )

    where the number of UUIDs in the key is a configurable parameter
    of each table, and the "val" is always binary and might be a
    trivial value, like 1.

    In one of the first programs using this package, TreeID and TimeID
    were just UUID that we can order lexically.

    inbound  = dict((TreeID, TimeID, TreeID) = b'')
    edges    = dict((TreeID, TimeID) = bytes((TreeID, TimeID)) )
    vertexes = dict((TreeID, TimeID) = binary)
    leafs    = dict(TreeID = binary)
    meta     = dict(TreeID = binary)


    Thus, the only thing that we need storage instances to do is
    provide a set of generalized "tables" of this form.
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        '''Initialize a storage instance for namespace'''
        self._config = config
        self._table_names = {}
        self._namespace = config.get('namespace', None)
        if not self._namespace:
            raise ProgrammerError('kvlayer requires a namespace')

    @abc.abstractmethod
    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.

        :param table_names: Each string in table_names becomes the
        name of a table, and the value must be an integer specifying
        the number of UUIDs in the keys

        :type table_names: dict(str = int)
        '''
        return

    @abc.abstractmethod
    def delete_namespace(self):
        '''Deletes all data from namespace.'''
        return

    @abc.abstractmethod
    def clear_table(self, table_name):
        'Delete all data from one table'
        return

    @abc.abstractmethod
    def put(self, table_name, *keys_and_values, **kwargs):
        '''Save values for keys in table_name.  Each key must be a
        tuple of UUIDs of the length specified for table_name in
        setup_namespace.

        :params batch_size: a DB-specific parameter that limits the
        number of (key, value) paris gathered into each batch for
        communication with DB.
        '''
        return

    @abc.abstractmethod
    def get(self, table_name, *key_ranges, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys within the specified ranges.  If no key_ranges
        are provided, then yield all (key, value) pairs in table.

        :type key_ranges: (((UUID, ...), (UUID, ...)), ...)
                            ^^^^^^^^^^^^^^^^^^^^^^^^
                            start        finish of one range
        '''
        return

    @abc.abstractmethod
    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        :params batch_size: a DB-specific parameter that limits the
        number of (key, value) paris gathered into each batch for
        communication with DB.
        '''
        return

    @abc.abstractmethod
    def close(self):
        '''
        close connections and end use of this storage client
        '''
        return
