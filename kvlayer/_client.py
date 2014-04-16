'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import argparse
from cStringIO import StringIO
import logging
import sys
import termios
import tty
import uuid

import yaml

import kvlayer
from kvlayer._accumulo import AStorage
from kvlayer._cassandra import CStorage
from kvlayer._local_memory import LocalStorage
from kvlayer._file_storage import FileStorage
from kvlayer._redis import RedisStorage
import yakonfig
from yakonfig.cmd import ArgParseCmd

try:
    from kvlayer._postgres import PGStorage
except ImportError:
    PGStorage = None

logger = logging.getLogger(__name__)

STORAGE_CLIENTS = dict(
    ## these strings deinfe the external API for selecting the kvlayer
    ## storage backends
    cassandra=CStorage,
    accumulo=AStorage,
    local=LocalStorage,
    filestorage=FileStorage,
    redis=RedisStorage
)

if PGStorage:
    STORAGE_CLIENTS['postgres'] = PGStorage

def client():
    '''
    constructs a storage client for the storage_type specified in config
    '''
    config = yakonfig.get_global_config('kvlayer')
    try:
        return STORAGE_CLIENTS[config['storage_type']]()
    except Exception, exc:
        logger.critical('config = %r' % config, exc_info=True)
        raise

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
            response = raw_input('Delete everything in {!r}?  Enter namespace: '
                                 .format(namespace))
            if response != namespace:
                self.stdout.write('not deleting anything\n')
                return
        self.stdout.write('deleting namespace {!r}\n'.format(namespace))
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
            self.stdout.write('{!r}\n'.format(k))

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
                self.stdout.write('No values for key {!r}.\n'.format(k))
            else:
                self.stdout.write('{!r}\n'.format(v))

def main():
    parser = argparse.ArgumentParser()
    action = Action()
    action.add_arguments(parser)
    args = yakonfig.parse_args(parser, [yakonfig, kvlayer])
    action.main(args)

if __name__ == '__main__':
    main()
