'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import argparse
import logging
import uuid

import pkg_resources

try:
    import dblogger
except ImportError:
    dblogger = None

import kvlayer
from kvlayer._accumulo import AStorage
from kvlayer._local_memory import LocalStorage
from kvlayer._file_storage import FileStorage
from kvlayer._redis import RedisStorage
import yakonfig
from yakonfig import ConfigurationError
from yakonfig.cmd import ArgParseCmd
from yakonfig.merge import overlay_config

try:
    from kvlayer._cassandra import CStorage
except ImportError:
    CStorage = None

try:
    from kvlayer._postgres import PGStorage
except ImportError:
    PGStorage = None

try:
    from kvlayer._postgrest import PostgresTableStorage
except ImportError:
    PostgresTableStorage = None

try:
    from kvlayer._riak import RiakStorage
except ImportError:
    RiakStorage = None

try:
    from kvlayer._split_s3 import SplitS3Storage
except ImportError:
    SplitS3Storage = None

try:
    from kvlayer._hbase import HBaseStorage
except ImportError:
    HBaseStorage = None

try:
    from kvlayer._cbor_proxy import CborProxyStorage
except ImportError:
    CborProxyStorage = None

logger = logging.getLogger(__name__)

STORAGE_CLIENTS = dict(
    ## these strings deinfe the external API for selecting the kvlayer
    ## storage backends
    accumulo=AStorage,
    local=LocalStorage,
    filestorage=FileStorage,
    redis=RedisStorage
)

if CStorage:
    STORAGE_CLIENTS['cassandra'] = CStorage
if PGStorage:
    STORAGE_CLIENTS['postgres'] = PGStorage
if PostgresTableStorage:
    STORAGE_CLIENTS[PostgresTableStorage.config_name] = PostgresTableStorage
if RiakStorage:
    STORAGE_CLIENTS['riak'] = RiakStorage
if SplitS3Storage:
    STORAGE_CLIENTS[SplitS3Storage.config_name] = SplitS3Storage
if HBaseStorage:
    STORAGE_CLIENTS['hbase'] = HBaseStorage
if CborProxyStorage:
    STORAGE_CLIENTS['cborproxy'] = CborProxyStorage

def load_entry_point_kvlayer_impls():
    for entry_point in pkg_resources.iter_entry_points('kvlayer.impl'):
        try:
            name = entry_point.name
            constructor = entry_point.load()
            if name in STORAGE_CLIENTS:
                logger.warn('kvlayer impl %r %s replaces existing impl %s', name, constructor, STORAGE_CLIENTS[name])
            STORAGE_CLIENTS[name] = constructor
        except:
            logger.error('failed loading kvlayer impl %r', entry_point and entry_point.name, exc_info=True)


_load_entry_point_kvlayer_impls_done = False


def client(config=None, storage_type=None, *args, **kwargs):
    '''Create a kvlayer client object.

    With no arguments, gets the global :mod:`kvlayer` configuration
    from :mod:`yakonfig` and uses that.  A `config` dictionary, if
    provided, is used in place of the :mod:`yakonfig` configuration.
    `storage_type` overrides the corresponding field in the
    configuration, but it must be supplied in one place or the other.
    Any additional parameters are passed to the corresponding
    backend's constructor.

    >>> local_storage = kvlayer.client(config={}, storage_type='local',
    ...                                app_name='app', namespace='ns')

    If there is additional configuration under the value of
    `storage_type`, that is overlaid over `config` and passed to the
    storage implementation.

    :param dict config: :mod:`kvlayer` configuration dictionary
    :param str storage_type: name of storage implementation
    :raise kvlayer._exceptions.ConfigurationError: if `storage_type`
      is not provided or is invalid

    '''
    global _load_entry_point_kvlayer_impls_done
    if config is None:
        config = yakonfig.get_global_config('kvlayer')
    if storage_type is None:
        try:
            storage_type = config['storage_type']
        except KeyError, exc:
            raise ConfigurationError(
                'No storage_type in kvlayer configuration')
    if storage_type in config:
        config = overlay_config(config, config[storage_type])

    if (storage_type not in STORAGE_CLIENTS) and (not _load_entry_point_kvlayer_impls_done):
        load_entry_point_kvlayer_impls()
        _load_entry_point_kvlayer_impls_done = True

    if storage_type not in STORAGE_CLIENTS:
        raise ConfigurationError('Invalid kvlayer storage_type {0!r}'
                                 .format(storage_type))

    cls = STORAGE_CLIENTS[storage_type]
    return cls(config, *args, **kwargs)

class Actions(ArgParseCmd):
    def __init__(self, *args, **kwargs):
        ArgParseCmd.__init__(self, *args, **kwargs)
        self.prompt = 'kvlayer> '
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = kvlayer.client()
        return self._client

    def args_delete(self, parser):
        parser.add_argument('-y', '--yes', default=False, action='store_true',
                            dest='assume_yes',
                            help='assume "yes" and require no input for '
                            'confirmation questions.')
    def do_delete(self, args):
        '''delete all tables in the current namespace'''
        namespace = self.client._namespace
        if not args.assume_yes:
            response = raw_input('Delete everything in {0!r}?  Enter namespace: '
                                 .format(namespace))
            if response != namespace:
                self.stdout.write('not deleting anything\n')
                return
        self.stdout.write('deleting namespace {0!r}\n'.format(namespace))
        self.client.delete_namespace()

    @staticmethod
    def _schema(s):
        n = { 'uuid': uuid.UUID, 'int': int, 'long': long, 'str': str }
        if s.isdigit(): return (uuid.UUID,) * int(s)
        if s in n: return (n[s],)
        if s.startswith('(') and s.endswith(')'): s=s[1:-1]
        parts = s.split(',')
        return tuple(n[p] for p in parts)

    def args_keys(self, parser):
        parser.add_argument('table', help='name of kvlayer table')
        parser.add_argument('schema', help='description of table keys',
                            type=self._schema)
    def do_keys(self, args):
        '''list all keys in a single table'''
        self.client.setup_namespace({ args.table: args.schema })
        for k,v in self.client.scan(args.table):
            self.stdout.write('{0!r}\n'.format(k))

    def args_get(self, parser):
        parser.add_argument('table', help='name of kvlayer table')
        parser.add_argument('schema', help='description of table keys',
                            type=self._schema)
        parser.add_argument('keys', nargs='+',
                            help='key to fetch')
    def do_get(self, args):
        '''get values from a single key'''
        self.client.setup_namespace({ args.table: args.schema })
        key = tuple(f(x) for f, x in zip(args.schema, args.keys))
        for k,v in self.client.get(args.table, key):
            if v is None:
                self.stdout.write('No values for key {0!r}.\n'.format(k))
            else:
                self.stdout.write('{0!r}\n'.format(v))


def main():
    parser = argparse.ArgumentParser()
    action = Actions()
    action.add_arguments(parser)
    modules = [yakonfig]
    if dblogger:
        modules += [dblogger]
    modules += [kvlayer]
    args = yakonfig.parse_args(parser, modules)
    action.main(args)

if __name__ == '__main__':
    main()
