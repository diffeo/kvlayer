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

def stderr(m, newline='\n'):
    sys.stderr.write(m)
    sys.stderr.write(newline)
    sys.stderr.flush()

def getch():
    '''
    capture one char from stdin for responding to Y/N prompt
    '''
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch

class Actions:
    @classmethod
    def names(cls):
        return [k[3:] for k in dir(cls) if k.startswith('do_')]

    @staticmethod
    def do_delete(kvlayer_client, args):
        stderr('Delete everything in %r?  Enter namespace: ' % kvlayer_client._namespace, newline='')
        if args.assume_yes:
            stderr('... assuming yes.\n')
            do_delete = True
        else:
            idx = 0
            assert len(kvlayer_client._namespace) > 0
            while idx < len(kvlayer_client._namespace):
                ch = getch()
                if ch == kvlayer_client._namespace[idx]:
                    idx += 1
                    do_delete = True
                else:
                    do_delete = False
                    break

        if do_delete:
            stderr('\nDeleting ...')
            sys.stdout.flush()
            kvlayer_client.delete_namespace()
            stderr('')

        else:
            stderr(' ... Aborting.')

    @staticmethod
    def do_keys(kvlayer_client, args):
        if len(args.args) < 2:
            print "usage: kvlayer keys table size [table size...]"
            return
        def schema(s):
            n = { 'uuid': uuid.UUID, 'int': int, 'long': long, 'str': str }
            if s.isdigit(): return s
            if s in n: return (n[s],)
            if s.startswith('(') and s.endswith(')'): s=s[1:-1]
            parts = s.split(',')
            return tuple(n[p] for p in parts)
        tables = dict(zip(args.args[0::2],
                          [schema(a) for a in args.args[1::2]]))
        kvlayer_client.setup_namespace(tables)
        for table in args.args[0::2]:
            print '{}:'.format(table)
            for k,v in kvlayer_client.scan(table):
                print '  {!r}'.format(k)
            print

def main():
    parser = argparse.ArgumentParser()
    ## TODO: implement "list" tables in namespace
    parser.add_argument('action', help='|'.join(Actions.names()))
    parser.add_argument('args', nargs='*', help='action-specific arguments')
    parser.add_argument('-y', '--yes', default=False, action='store_true', dest='assume_yes',
                        help='Assume "yes" and require no input for confirmation questions.')
    args = yakonfig.parse_args(parser, [yakonfig, kvlayer])
    kvlayer_client = client()

    f = getattr(Actions, 'do_' + args.action, None)
    if f is None:
        parser.error('invalid action {!r}; allowed values are {}'
                     .format(args.action, ', '.join(Actions.names())))
    f(kvlayer_client, args)
