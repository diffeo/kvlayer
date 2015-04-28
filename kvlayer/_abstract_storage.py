'''
Definition of AbstractStorage, which all storage
implementations inherit.

Your use of this software is governed by your license agreement.

Copyright 2012-2015 Diffeo, Inc.
'''

from __future__ import absolute_import
import abc
import atexit
import collections
import itertools
import json
import operator
import struct
import time
import uuid

from kvlayer.encoders import get_encoder
from kvlayer._exceptions import BadKey, ConfigurationError, ProgrammerError


class COUNTER(object):
    '''Integer counter value type.

    You cannot meaningfully instantiate this class.  Instead, pass
    it as a value in the `value_types` dictionary parameter to
    :meth:`AbstractStorage.setup_namespace`.

    If a table has this value type, then its values are integers,
    in the same way as if the table had value type
    :class:`int`; but you can also use the table as a counter
    using :meth:`AbstractStorage.increment`.

    '''


class ACCUMULATOR(object):
    '''Floating-point counter value type.

    You cannot meaningfully instantiate this class.  Instead, pass
    it as a value in the `value_types` dictionary parameter to
    :meth:`AbstractStorage.setup_namespace`.

    If a table has this value type, then its values are floating-point
    numbers, in the same way as if the table had value type
    :class:`float`; but you can also use the table as a counter using
    :meth:`AbstractStorage.increment`.

    '''


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

    def check_put_key_value(self, key, value, table_name, key_spec=None,
                            value_type=None):
        '''Check that a key/value pair are consistent with the schema.

        :param tuple key: key to put
        :param value: value to put (ignored)
        :param str table_name: kvlayer table name (for errors only)
        :param tuple key_spec: definition of the table key
        :param value_type: type of the table value
        :raise kvlayer._exceptions.BadKey: if `key` doesn't match `key_spec`

        '''
        if key_spec is None:
            key_spec = self._table_names[table_name]
        if value_type is None:
            value_type = self._value_types[table_name]
        if value_type is COUNTER:
            value_type = int
        if value_type is ACCUMULATOR:
            value_type = float
        if not isinstance(key, tuple):
            raise BadKey('key should be tuple, but got %s' % (type(key),))
        if len(key) != len(key_spec):
            raise BadKey('%r wants %r parts in key tuple, but got %r' %
                         (table_name,  len(key_spec), len(key)))
        for kp, ks in zip(key, key_spec):
            if not isinstance(kp, ks):
                raise BadKey('part of key wanted type %s but got %s: %r' %
                             (ks, type(kp), kp))
        if not isinstance(value, value_type):
            raise BadKey('value should be %s, but got %s' %
                         (value_type, type(value)))

    def value_to_str(self, value, value_type):
        if value is None:
            return None
        if value_type is str:
            return value
        if value_type is int or value_type is COUNTER:
            return struct.pack('>i', value)
        if value_type is float or value_type is ACCUMULATOR:
            return struct.pack('>f', value)
        raise ConfigurationError('unexpected value_type {0!r}'
                                 .format(value_type))

    def str_to_value(self, value, value_type):
        if value is None:
            return None
        if value_type is str:
            return value
        if value_type is int or value_type is COUNTER:
            if len(value) == 4:
                return struct.unpack('>i', value)[0]
            elif len(value) == 8:
                return struct.unpack('>q', value)[0]
        if value_type is float or value_type is ACCUMULATOR:
            return struct.unpack('>f', value)[0]
        raise ConfigurationError('unexpected value_type {0!r}'
                                 .format(value_type))

    def __init__(self, config, app_name=None, namespace=None):
        '''Initialize a storage instance with config dict.

        `config` is the configuration for this object; it may come
        from :mod:`yakonfig` or elsewhere.  If `app_name` or `namespace`
        are given as arguments to this method, they override the
        corresponding parameters in the configuration.  These two
        parameters _must_ be present, if not from the named arguments
        then in the configuration, otherwise :class:`ConfigurationError`
        will be raised.

        This understands the following keys in `config`:

        `app_name`
          wrapper name for all namespaces
        `namespace`
          wrapper name for all managed tables
        `storage_addresses`
          list of locations of data storage; backend-specific
        `log_stats`
          if provided, name of a file to which to log statistics
        `log_stats_interval_ops`
          if provided, log stats after this many operations
        `log_stats_interval_seconds`
          if provided, log stats after this many seconds
        `encoder`
          name of a key-to-string encoder, if applicable

        :param dict config: local configuration dictionary
        :param str app_name: optional app name override
        :param str namespace: optional namespace override
        :param kvlayer.encoders.base.Encoder encoder: key serializer
        :raise kvlayer._exceptions.ConfigurationError: if no `app_name`
          or `namespace` could be found
        '''
        self._config = config
        self._table_names = {}
        self._value_types = {}
        self._namespace = namespace or self._config.get('namespace', None)
        if not self._namespace:
            raise ConfigurationError('kvlayer requires a namespace')
        self._app_name = app_name or self._config.get('app_name', None)
        if not self._app_name:
            raise ConfigurationError('kvlayer requires an app_name')
        self._encoder = get_encoder(self._config.get('encoder', None))
        self._require_uuid = self._config.get('keys_must_be_uuid', True)
        log_stats_cfg = self._config.get('log_stats', None)
        # StorageStats also consumes:
        #  log_stats_interval_ops
        #  log_stats_interval_seconds
        if log_stats_cfg:
            self._log_stats = StorageStats(log_stats_cfg, self._config)
        else:
            self._log_stats = None

    def setup_namespace(self, table_names, value_types=None):
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

        `value_types` specifies the type of the values for a given
        table.  Tables default to having a value type of :class:`str`.
        :class:`int` and :class:`float` are also permitted.  Value
        types may also be :class:`COUNTER` or :class:`ACCUMULATOR`;
        see :meth:`increment` for details on these types.  You must pass
        the corresponding type as the value parameter to :meth:`put`,
        and that type will be returned as the value part of
        :meth:`get` and :meth:`scan`.

        :param dict table_names: Mapping from table name to key type tuple
        :param dict value_types: Mapping from table name to value type

        '''
        if value_types is None:
            value_types = {}
        # Subclass implementations should call this superclass
        # implementation to actually populate self._table_names.
        for k, v in table_names.iteritems():
            if isinstance(v, (int, long)):
                if v >= 50:
                    # This is probably a bug
                    raise ConfigurationError(
                        'excessively long tuple size {0!r} for table {1!r}'
                        .format(v, k))
                v = (uuid.UUID,) * v
            self._table_names[k] = v
            value_type = value_types.get(k, str)
            if value_type not in (str, int, float, COUNTER, ACCUMULATOR):
                raise ConfigurationError(
                    'invalid value type {0!r} for table {1!r}'
                    .format(value_type, k))
            self._value_types[k] = value_type

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

    def log_put(self, table_name, start_time, end_time, num_keys, keys_size,
                num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.put.add(table_name, start_time, end_time, num_keys,
                                    keys_size, num_values, values_size)

    @abc.abstractmethod
    def scan(self, table_name, *key_ranges, **kwargs):
        '''Yield tuples of (key, value) from querying table_name for
        items with keys within the specified ranges.  If no key_ranges
        are provided, then yield all (key, value) pairs in table.
        This may return nothing if the table is empty or there are
        no matching keys in any of the specified ranges.

        Each of the `key_ranges` is a pair of a start and end tuple to
        scan.  To specify the beginning or end, a -Inf or Inf value,
        use an empty tuple as the beginning or ending key of a range.

        '''
        return

    def log_scan(self, table_name, start_time, end_time, num_keys, keys_size,
                 num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.scan.add(
                table_name, start_time, end_time,
                num_keys, keys_size, num_values, values_size)

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        '''Scan only the keys from a table.

        Yields key tuples from queying `table_name` for keys within
        the specified ranges.  If no `key_ranges` are provided, then
        yield all key tuples in the table.  This may yield nothing if
        the table is empty or there are no matching keys in any of the
        specified ranges.

        Each of the `key_ranges` is a pair of a start and end tuple to
        scan.  To specify the beginning or end, a -Inf or Inf value,
        use an empty tuple as the beginning or ending key of a range.

        '''
        # Feel free to reimplement this if your backend can do better!
        # we don't do log_scan_keys() here, let underlying scan() call
        # log_scan()
        return itertools.imap(operator.itemgetter(0),
                              self.scan(table_name, *key_ranges, **kwargs))

    def log_scan_keys(self, table_name, start_time, end_time, num_keys,
                      keys_size):
        if self._log_stats is not None:
            self._log_stats.scan_keys.add(table_name, start_time, end_time,
                                          num_keys, keys_size, 0, 0)

    @abc.abstractmethod
    def get(self, table_name, *keys, **kwargs):

        '''Yield tuples of (key, value) from querying table_name for items
        with keys.  If any of the key tuples are not in the table,
        those key tuples will be yielded with value :const:`None`.

        '''
        return

    def log_get(self, table_name, start_time, end_time, num_keys, keys_size,
                num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.get.add(
                table_name, start_time, end_time,
                num_keys, keys_size, num_values, values_size)

    @abc.abstractmethod
    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        '''
        return

    def log_delete(self, table_name, start_time, end_time, num_keys,
                   keys_size):
        if self._log_stats is not None:
            self._log_stats.delete.add(table_name, start_time, end_time,
                                       num_keys, keys_size, 0, 0)

    @abc.abstractmethod
    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self._log_stats is not None:
            self._log_stats.close()
            self._log_stats = None

    def increment(self, table_name, *keys_and_values):
        '''Add values to a counter-type table.

        `keys_and_values` are parameters of `(key, value)` pairs.  The
        values must be :class:`int`, if `table_name` is a
        :class:`COUNTER` table, or :class:`float`, if `table_name` is
        a :class:`ACCUMULATOR` table.  For each key, the current value
        is fetched from the storage, the value is added to it, and the
        resulting value added back into the storage.

        This method is not guaranteed to be atomic, either on a
        specific key or across all keys, but specific backends may
        have better guarantees.  The behavior is unspecified if the
        same key is included multiple times in the parameter list.

        To use this, you must have passed `table_name` to
        :meth:`setup_namespaces` in its `value_types` parameter,
        setting the value type to :data:`COUNTER` or
        :data:`ACCUMULATOR`.  When you do this, you pass and receive
        :class:`float` values back from all methods in this class for
        that table.  :meth:`put` directly sets the values of counter
        keys.  Counter values default to 0; if you change a counter
        value to 0 then it will be "present" for purposes of
        :meth:`get`, :meth:`scan`, and :meth:`scan_keys`.

        :param str table_name: name of table to update
        :param keys_and_values: additional parameters are pairs of
          key tuple and numeric delta value

        '''
        if self._value_types[table_name] not in [COUNTER, ACCUMULATOR]:
            raise ProgrammerError('table {0} is not a counter table'
                                  .format(table_name))
        # Default, non-atomic implementation
        keys = [k for (k, v) in keys_and_values]
        deltas = [v for (k, v) in keys_and_values]
        old_keys_values = self.get(table_name, *keys)
        new_keys_values = [(k, (v or 0)+d)
                           for ((k, v), d) in zip(old_keys_values, deltas)]
        self.put(table_name, *new_keys_values)


class StringKeyedStorage(AbstractStorage):
    '''Partial implementation of AbstractStorage using string keys.

    This assumes that the underlying database only can deal in byte-string
    keys, and :attr:`encoder` needs to be used to generate keys and values.
    This provides wrappers that do the encoding and basic stats gathering,
    requiring derived classes to only provide the underlying machinery.

    '''

    def put(self, table_name, *keys_and_values, **kwargs):
        start_time = time.time()
        for (k, v) in keys_and_values:
            self.check_put_key_value(k, v, table_name)
        ks = [self._encoder.serialize(k, self._table_names[table_name])
              for (k, v) in keys_and_values]
        vs = [self.value_to_str(v, self._value_types[table_name])
              for (k, v) in keys_and_values]
        self._put(table_name, zip(ks, vs), **kwargs)
        end_time = time.time()
        if self._log_stats is not None:
            self._log_stats.put.add(table_name, start_time, end_time,
                                    len(ks), sum(len(k) for k in ks),
                                    len(vs), sum(len(v) for v in vs))

    @abc.abstractmethod
    def _put(self, table_name, keys_and_values):
        pass

    def scan(self, table_name, *key_ranges, **kwargs):
        stats = StatRecord()
        key_spec = self._table_names[table_name]
        value_type = self._value_types[table_name]
        new_key_ranges = [(self._encoder.make_start_key(start, key_spec),
                           self._encoder.make_end_key(end, key_spec))
                          for (start, end) in key_ranges]
        for k, v in self._scan(table_name, new_key_ranges, **kwargs):
            stats.record(len(k), len(v))
            yield (self._encoder.deserialize(k, key_spec),
                   self.str_to_value(v, value_type))
        if self._log_stats is not None:
            self._log_stats.scan.add_rec(table_name, stats)

    @abc.abstractmethod
    def _scan(self, table_name, key_ranges):
        pass

    def scan_keys(self, table_name, *key_ranges, **kwargs):
        stats = StatRecord()
        key_spec = self._table_names[table_name]
        new_key_ranges = [(self._encoder.make_start_key(start, key_spec),
                           self._encoder.make_end_key(end, key_spec))
                          for (start, end) in key_ranges]
        for k in self._scan_keys(table_name, new_key_ranges, **kwargs):
            stats.record(len(k), None)
            yield self._encoder.deserialize(k, key_spec)
        if self._log_stats is not None:
            self._log_stats.scan_keys.add_rec(table_name, stats)

    def _scan_keys(self, table_name, key_ranges):
        for (k, v) in self._scan(table_name, key_ranges):
            yield k

    def get(self, table_name, *keys, **kwargs):
        stats = StatRecord()
        key_spec = self._table_names[table_name]
        value_type = self._value_types[table_name]
        new_keys = [self._encoder.serialize(k, key_spec) for k in keys]
        for (k, v) in self._get(table_name, new_keys, **kwargs):
            if v is None:
                stats.record(len(k), None)
            else:
                stats.record(len(k), len(v))
            yield (self._encoder.deserialize(k, key_spec),
                   self.str_to_value(v, value_type))
        if self._log_stats is not None:
            self._log_stats.get.add_rec(table_name, stats)

    @abc.abstractmethod
    def _get(self, table_name, keys):
        pass

    def delete(self, table_name, *keys, **kwargs):
        start_time = time.time()
        key_spec = self._table_names[table_name]
        new_keys = [self._encoder.serialize(k, key_spec) for k in keys]
        self._delete(table_name, new_keys, **kwargs)
        if self._log_stats is not None:
            self._log_stats.delete.add(
                table_name, start_time, time.time(),
                len(new_keys), sum(len(k) for k in new_keys), 0, 0)

    @abc.abstractmethod
    def _delete(self, table_name, keys):
        pass


# I may have built this inside-out.
# Maybe the top level split should be on table, and keep stats of ops within
# that.  It shouldn't be hard to transpose for display if needed.
class StorageStats(object):
    def __init__(self, config_str_or_writeable, config):
        self._f = None
        self._config_str = None
        if hasattr(config_str_or_writeable, 'write'):
            self._f = config_str_or_writeable
            self._write_json = config.get('json', False)
        elif isinstance(config_str_or_writeable, (str, unicode)):
            self._config_str = config_str_or_writeable
            self._write_json = self._config_str.endswith('.json') or config.get('json', False)
        self._interval_ops = config.get('log_stats_interval_ops')
        if self._interval_ops is not None:
            self._interval_ops = int(self._interval_ops)
            if self._interval_ops <= 0:
                self._interval_ops = None
        self._op_interval_counter = 0
        self._interval_seconds = config.get('log_stats_interval_seconds')
        if self._interval_seconds is not None:
            self._interval_seconds = float(self._interval_seconds)
            if self._interval_seconds <= 0.0:
                self._interval_seconds = None
        self._interval_s_last_flushed = time.time()
        self.put = OpStats(self)
        self.scan = OpStats(self)
        self.scan_keys = OpStats(self)
        self.get = OpStats(self)
        self.delete = OpStats(self)

        self._closed = False
        atexit.register(self.atexit)

    def __str__(self):
        outparts = []
        if self.put.num_ops:
            outparts.append('put:')
            outparts.append(str(self.put))
        if self.scan.num_ops:
            outparts.append('scan:')
            outparts.append(str(self.scan))
        if self.scan_keys.num_ops:
            outparts.append('scan_keys:')
            outparts.append(str(self.scan_keys))
        if self.get.num_ops:
            outparts.append('get:')
            outparts.append(str(self.get))
        if self.delete.num_ops:
            outparts.append('delete:')
            outparts.append(str(self.delete))
        return '\n'.join(outparts) + '\n'

    def to_dict(self):
        "return a dict suitable for json.dump()"
        out = {}
        if self.put.num_ops:
            out['put'] = self.put.to_dict()
        if self.scan.num_ops:
            out['scan'] = self.scan.to_dict()
        if self.scan_keys.num_ops:
            out['scan_keys'] = self.scan_keys.to_dict()
        if self.get.num_ops:
            out['get'] = self.get.to_dict()
        if self.delete.num_ops:
            out['delete'] = self.delete.to_dict()
        return out

    def _out(self):
        if (self._f is None) and hasattr(self._config_str, 'write'):
            self._f = self._config_str
        if self._f is None:
            self._f = open(self._config_str, 'a')
        return self._f

    def did_op(self):
        if self._closed:
            return
        if self._interval_ops is not None:
            self._op_interval_counter += 1
            if self._op_interval_counter >= self._interval_ops:
                self.flush()
                self._interval_s_last_flushed = time.time()
                self._op_interval_counter = 0
                return
        if self._interval_seconds is not None:
            now = time.time()
            if self._interval_s_last_flushed + self._interval_seconds < now:
                self.flush()
                self._interval_s_last_flushed = now
                self._op_interval_counter = 0
                return

    def write_json(self, writeable):
        data = self.to_dict()
        now = time.time()
        data['time'] = now
        data['times'] = time.strftime('%Y%m%d_%H%M%S', time.gmtime(now))
        writeable.write(json.dumps(data) + '\n')

    def flush(self):
        out = self._out()
        if self._write_json:
            self.write_json(out)
        else:
            out.write(time.strftime('%Y%m%d_%H%M%S\n'))
            out.write(self.__str__())
        if hasattr(out, 'flush'):
            out.flush()

    def close(self):
        if self._f is not None:
            if hasattr(self._f, 'close'):
                self._f.close()
            self._f = None
        self._closed = True

    def atexit(self):
        if not self._closed:
            self.flush()
            self.close()


class OpStats(object):
    def __init__(self, parentStorageStats):
        self.by_table = collections.defaultdict(OpStatsPerTable)
        self.num_ops = 0
        self._psto = parentStorageStats

    def add(self, table_name, start_time, end_time, num_keys, keys_size,
            num_values, values_size):
        self.num_ops += 1
        self.by_table[table_name].add(start_time, end_time,
                                      num_keys, keys_size,
                                      num_values, values_size)
        self._psto.did_op()

    def add_rec(self, table_name, record):
        '''Variant of :meth:`add` taking a :class:`StatRecord`.'''
        return self.add(table_name, record.start_time, time.time(),
                        record.num_keys, record.keys_size,
                        record.num_values, record.values_size)

    def __str__(self):
        parts = []
        total = OpStatsPerTable()
        for k, v in self.by_table.iteritems():
            total += v
            parts.append('{0:10s} {1}\n'.format(k, str(v)))
        return ''.join(parts) + '           {0}\n'.format(str(total))

    def to_dict(self):
        "return a dict suitable for json.dump()"
        out = {}
        for k, v in self.by_table.iteritems():
            out[k] = v.to_dict()
        return out


class OpStatsPerTable(object):
    def __init__(self):
        self.total_time = 0.0
        self.num_ops = 0
        self.num_keys = 0
        self.keys_size = 0
        self.num_values = 0
        self.values_size = 0

    def add(self, start_time, end_time, num_keys, keys_size, num_values,
            values_size=0):
        self.total_time += (end_time - start_time)
        self.num_ops += 1
        self.num_keys += num_keys
        self.keys_size += keys_size
        self.num_values += num_values
        self.values_size += values_size

    def __iadd__(self, b):
        assert isinstance(b, OpStatsPerTable)
        self.total_time += b.total_time
        self.num_ops += b.num_ops
        self.num_keys += b.num_keys
        self.keys_size += b.keys_size
        self.num_values += b.num_values
        self.values_size += b.values_size
        return self

    def __str__(self):
        out = ('{t:0.3f}s on {ops} ops ({spops:0.6g} s/op), '
               '{k} keys for {kb} bytes ({bpk} B/key)'.format(
                   t=self.total_time,
                   ops=self.num_ops,
                   spops=self.num_ops and (self.total_time/self.num_ops),
                   k=self.num_keys,
                   kb=self.keys_size,
                   bpk=self.num_keys and ((1.0*self.keys_size)/self.num_keys)
               ))
        if self.num_values > 0:
            out += '{v} values for {vb} bytes ({bpv:0.1f} B/val)'.format(
                v=self.num_values,
                vb=self.values_size,
                bpv=(1.0*self.values_size)/self.num_values
            )
        if self.total_time > 0:
            out += ' {0:0.1f} (k+v)B/s'.format(
                (1.0*(self.keys_size + self.values_size))/self.total_time)
        return out

    def to_dict(self):
        "return a dict suitable for json.dump()"
        out = {
            't': self.total_time,
            'ops': self.num_ops,
            'k': self.num_keys,
            'kb': self.keys_size,
        }
        if self.num_values > 0:
            out['v'] = self.num_values
            out['vb'] = self.values_size
        return out


class StatRecord(object):
    '''Combined statistics record for a single action.'''

    def __init__(self):
        #: Starting time of the action
        self.start_time = time.time()
        #: Number of keys processed
        self.num_keys = 0
        #: Total byte size of keys processed
        self.keys_size = 0
        #: Number of values processed
        self.num_values = 0
        #: Total byte size of values processed
        self.values_size = 0

    def record(self, key_size, value_size):
        '''Add a single item to the record.

        Either `key_size` or `value_size` may be :const:`None`; if so then
        don't count that part.  :meth:`AbstractStorage.scan_keys`
        implementations will typically pass :const:`None` for `value_size`,
        for instance.  Otherwise increment the relevant counts and add
        the relevant byte sizes.

        '''
        if key_size is not None:
            self.num_keys += 1
            self.keys_size += key_size
        if value_size is not None:
            self.num_values += 1
            self.values_size += value_size
