'''
Definition of AbstractStorage, which all storage
implementations inherit.

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import abc
import uuid

from kvlayer._exceptions import BadKey, ProgrammerError
import yakonfig

class AbstractStorage(object):
    '''Base class for all low-level storage implementations.

    All of the table-like structures we use are setup like this::

        namespace = dict(
            table_name = dict((UUID, UUID, ...): val)
            ...
        )

    where the number of UUIDs in the key is a configurable parameter
    of each table, and the "val" is always binary and might be a
    trivial value, like 1.

    '''
    __metaclass__ = abc.ABCMeta

    def check_put_key_value(self, key, value, table_name, key_spec):
        "check that (key, value) are ok. return Exception or None if okay."
        if not isinstance(key, tuple):
            return BadKey('key should be tuple, but got %s' % (type(key),))
        if len(key) != len(key_spec):
            return BadKey('%r wants %r parts in key tuple, but got %r' % (table_name,  len(key_spec), len(key)))
        for kp, ks in zip(key, key_spec):
            if not isinstance(kp, ks):
                return BadKey('part of key wanted type %s but got %s' % (ks, type(kp)))
        return None

    @abc.abstractmethod
    def __init__(self):
        '''Initialize a storage instance with config dict.
        Typical config fields:
        'namespace': string name of set of tables this kvlayer instance refers to
        'app_name': string name of application code which is connecting
        'storage_addresses': [list of server specs]
        'username'
        'password'
        '''
        self._config = yakonfig.get_global_config('kvlayer')
        self._table_names = {}
        self._namespace = self._config.get('namespace', None)
        if not self._namespace:
            raise ProgrammerError('kvlayer requires a namespace')
        self._app_name = self._config.get('app_name', None)
        if not self._app_name:
            raise ProgrammerError('kvlayer requires an app_name')
        self._require_uuid = self._config.get('keys_must_be_uuid', True)

    @abc.abstractmethod
    def setup_namespace(self, table_names):
        '''Create tables in the namespace.

        Can be run multiple times with different `table_names` in
        order to expand the set of tables in the namespace.  This
        generally needs to be called by every client, even if only
        reading data.

        Tables are specified by the form of their keys. A key must be
        a tuple of a set number and type of parts. Currently types
        :class:`uuid.UUID`, :class:`int`, :class:`long`, and
        :class:`str` are well supported, anything else is serialzed by
        :func:`str`. Historically, a kvlayer key had to be a tuple of some
        number of UUIDs.  `table_names` is a dictionary mapping a
        table name to a tuple of types.  The dictionary values may also
        be integers, in which case the tuple is that many UUIDs.

        :param dict table_names: Mapping from table name to value type tuple

        '''
        return

    def normalize_namespaces(self, table_names):
        '''Normalize table_names spec dictionary in place.

        Replaces ints with a tuple of that many (uuid.UUID,)
        '''
        for k, v in table_names.iteritems():
            if isinstance(v, (int, long)):
                assert v < 50, "assuming attempt at very long key is a bug"
                table_names[k] = (uuid.UUID,) * v

    @abc.abstractmethod
    def delete_namespace(self):
        '''Deletes all data from namespace.'''
        return

    @abc.abstractmethod
    def clear_table(self, table_name):
        '''Delete all data from one table.'''
        return

    @abc.abstractmethod
    def put(self, table_name, *keys_and_values, **kwargs):
        '''Save values for keys in `table_name`.

        Each key must be a tuple of length and types as specified for
        `table_name` in :meth:`setup_namespace`.

        '''
        return

    @abc.abstractmethod
    def scan(self, table_name, *key_ranges, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys within the specified ranges.  If no key_ranges
        are provided, then yield all (key, value) pairs in table.

        Each of the `key_ranges` is a pair of a start and end tuple to
        scan.  To specify the beginning or end, a -Inf or Inf value,
        use an empty tuple as the beginning or ending key of a range.

        :raise kvlayer.MissingID: if at least one range was specified
          but no items in that range are present
        '''
        return

    @abc.abstractmethod
    def get(self, table_name, *keys, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys

        :type keys: (((UUID, ...), (UUID, ...)), ...)

        :raise kvlayer.MissingID: if at least one key is specified
          but no items with those keys can be found

        '''
        return

    @abc.abstractmethod
    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        '''
        return

    @abc.abstractmethod
    def close(self):
        '''
        close connections and end use of this storage client
        '''
        return
