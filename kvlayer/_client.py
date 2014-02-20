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

def client(config):
    '''
    constructs a storage client for the storage_type specified in config
    '''
    try:
        return STORAGE_CLIENTS[config['storage_type']](config)
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

allowed_actions = ['delete', 'dump_config']

def main():
    parser = argparse.ArgumentParser()
    ## TODO: implement "list" tables in namespace
    parser.add_argument('action', help='|'.join(allowed_actions))
    parser.add_argument('-y', '--yes', default=False, action='store_true', dest='assume_yes',
                        help='Assume "yes" and require no input for confirmation questions.')
    args = yakonfig.parse_args(parser, [yakonfig, kvlayer])
    config = yakonfig.get_global_config()
    kvlayer_client = client(config['kvlayer'])

    if args.action not in allowed_actions:
        sys.exit('only currently allowed actions are %r' % allowed_actions)

    elif args.action == 'dump_config':
        print yaml.dump(config)

    elif args.action == 'delete':
        stderr('Delete everything in %r?  Enter namespace: ' % args.namespace, newline='')
        if args.assume_yes:
            stderr('... assuming yes.\n')
            do_delete = True
        else:
            idx = 0
            assert len(args.namespace) > 0
            while idx < len(args.namespace):
                ch = getch()
                if ch == args.namespace[idx]:
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

