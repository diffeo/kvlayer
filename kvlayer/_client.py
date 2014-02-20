'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
import argparse
from cStringIO import StringIO
import logging
import sys
import termios
import tty
import yaml

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

logger = logging.getLogger('kvlayer')

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


default_config = dict(
    storage_type = 'local',
    connection_pool_size = 2,
    max_consistency_delay = 120,
    replication_factor = 1,
    thrift_framed_transport_size_in_mb = 15,
    )


def add_arguments(parser, defaults=None, include_app_name=False, include_namespace=False):
    '''
    add command line arguments to an argparse.ArgumentParser instance.
    This provides sensible defaults and accurate help messages, so
    that libraries that use kvlayer can provide these flags in their
    command line interfaces.
    '''
    if  defaults is None:
        defaults = dict()

    if include_app_name:
        parser.add_argument('--app-name', default=defaults.get('app_name'), 
                            help='name of app for namespace prefixing')
    if include_namespace:
        parser.add_argument('--namespace', default=None, help='namespace for prefixing table names')

    ## standard flags that are unique to kvlayer
    parser.add_argument('--storage-type', default=default_config.get('storage_type'), 
                        help='backend type for kvlayer, e.g. "local" or "accumulo"')
    parser.add_argument('--storage-address', action='append', default=[], dest='storage_addresses',
                        help='network addresses for kvlayer, can be repeated')

    parser.add_argument('--username', help='username for kvlayer accumulo')
    parser.add_argument('--password', help='password for kvlayer accumulo')

    parser.add_argument('--connection-pool-size', default=default_config.get('connection_pool_size'),
                        help='number of connections for kvlayer to open in advance')
    parser.add_argument('--max-consistency-delay', default=default_config.get('max_consistency_delay'),
                        help='number of seconds for kvlayer to wait for DB cluster sync')
    parser.add_argument('--replication-factor', default=default_config.get('replication_factor'),
                        help='number of copies of the data for kvlayer to require of DB cluster')
    parser.add_argument('--thrift-framed-transport-size-in-mb', default=default_config.get('thrift_framed_transport_size_in_mb'),
                        help='must not exceed value set on the server-side of DB cluster.  15MB is hardcoded default in thrift.')


def default_yaml():
    '''
    return default yaml string for use with yakonfig's !include_func
    '''
    return '''
app_name:  !runtime app_name
namespace: !runtime namespace
username: !runtime username
password: !runtime password
storage_type: !runtime storage_type
storage_addresses: !runtime storage_addresses
connection_pool_size: !runtime connection_pool_size
max_consistency_delay: !runtime max_consistency_delay
replication_factor: !runtime replication_factor
thrift_framed_transport_size_in_mb: !runtime thrift_framed_transport_size_in_mb
'''


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
    add_arguments(parser, include_app_name=True, include_namespace=True)
    args = parser.parse_args()

    yakonfig.set_runtime_args_object(args)

    fh = StringIO('kvlayer: !include_func kvlayer.default_yaml')
    config = yakonfig.set_global_config(fh)

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

