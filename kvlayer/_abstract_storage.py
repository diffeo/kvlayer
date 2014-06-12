'''
Definition of AbstractStorage, which all storage
implementations inherit.

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import abc
import atexit
import itertools
import operator
import time
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
                return BadKey('part of key wanted type %s but got %s: %r' % (ks, type(kp), kp))
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
        log_stats_cfg = self._config.get('log_stats', None)
        # StorageStats also consumes:
        #  log_stats_interval_ops
        #  log_stats_interval_seconds
        if log_stats_cfg:
            self._log_stats = StorageStats(log_stats_cfg, self._config)
        else:
            self._log_stats = None

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

    def log_put(self, table_name, start_time, end_time, num_keys, keys_size, num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.put.add(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

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

    def log_scan(self, table_name, start_time, end_time, num_keys, keys_size, num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.scan.add(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

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
        # we don't do log_scan_keys() here, let underlying scan() call log_scan()
        return itertools.imap(operator.itemgetter(0),
                              self.scan(table_name, *key_ranges, **kwargs))

    def log_scan_keys(self, table_name, start_time, end_time, num_keys, keys_size):
        if self._log_stats is not None:
            self._log_stats.scan_keys.add(table_name, start_time, end_time, num_keys, keys_size, 0, 0)

    @abc.abstractmethod
    def get(self, table_name, *keys, **kwargs):

        '''Yield tuples of (key, value) from querying table_name for items
        with keys.  If any of the key tuples are not in the table,
        those key tuples will be yielded with value :const:`None`.

        '''
        return

    def log_get(self, table_name, start_time, end_time, num_keys, keys_size, num_values, values_size):
        if self._log_stats is not None:
            self._log_stats.get.add(table_name, start_time, end_time, num_keys, keys_size, num_values, values_size)

    @abc.abstractmethod
    def delete(self, table_name, *keys, **kwargs):
        '''Delete all (key, value) pairs with specififed keys

        '''
        return

    def log_delete(self, table_name, start_time, end_time, num_keys, keys_size):
        if self._log_stats is not None:
            self._log_stats.delete.add(table_name, start_time, end_time, num_keys, keys_size, 0, 0)

    @abc.abstractmethod
    def close(self):
        '''
        close connections and end use of this storage client
        '''
        if self._log_stats is not None:
            self._log_stats.close()
            self._log_stats = None


# I may have built this inside-out.
# Maybe the top level split should be on table, and keep stats of ops within that.
# It shouldn't be hard to transpose for display if needed.
class StorageStats(object):
    def __init__(self, config_str_or_writeable, config):
        self._f = None
        self._config_str = None
        if hasattr(config_str_or_writeable, 'write'):
            self._f = config_str_or_writeable
        elif isinstance(config_str_or_writeable, (str, unicode)):
            self._config_str = config_str_or_writeable
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
        return 'put:\n{x.put}\nscan:\n{x.scan}\nscan_keys:\n{x.scan_keys}\nget:\n{x.get}\ndelete:\n{x.delete}\n'.format(x=self)

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
            if self._interval_s_last_flushed + self._interval_seconds < now():
                self.flush()
                self._interval_s_last_flushed = now
                self._op_interval_counter = 0
                return

    def flush(self):
        out = self._out()
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
        self.by_table = {}
        self.num_ops = 0
        self._psto = parentStorageStats

    def add(self, table_name, start_time, end_time, num_keys, keys_size, num_values, values_size):
        self.num_ops += 1
        ts = self.by_table.get(table_name)
        if ts is None:
            ts = OpStatsPerTable()
            self.by_table[table_name] = ts
        ts.add(start_time, end_time, num_keys, keys_size, num_values, values_size)
        self._psto.did_op()

    def __str__(self):
        parts = []
        total = OpStatsPerTable()
        for k,v in self.by_table.iteritems():
            total += v
            parts.append('{:10s} {}\n'.format(k, str(v)))
        return ''.join(parts) + '           {}\n'.format(str(total))


class OpStatsPerTable(object):
    def __init__(self):
        self.total_time = 0.0
        self.num_ops = 0
        self.num_keys = 0
        self.keys_size = 0
        self.num_values = 0
        self.values_size = 0

    def add(self, start_time, end_time, num_keys, keys_size, num_values, values_size=0):
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
        out = '{t:0.3f}s on {ops} ops ({spops:0.6g} s/op), {k} keys for {kb} bytes ({bpk} B/key)'.format(
            t=self.total_time,
            ops=self.num_ops,
            spops=self.num_ops and (self.total_time/self.num_ops),
            k=self.num_keys,
            kb=self.keys_size,
            bpk=self.num_keys and ((1.0*self.keys_size)/self.num_keys)
        )
        if self.num_values > 0:
            out += '{v} values for {vb} bytes ({bpv} B/val)'.format(
                v=self.num_values,
                vb=self.values_size,
                bpv=(1.0*self.values_size)/self.num_values
            )
        return out
