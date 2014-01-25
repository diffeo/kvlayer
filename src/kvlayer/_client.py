'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

from kvlayer._accumulo import AStorage
from kvlayer._cassandra import CStorage
from kvlayer._local_memory import LocalStorage
from kvlayer._file_storage import FileStorage

try:
    from kvlayer._postgres import PGStorage
except ImportError:
    PGStorage = None

STORAGE_CLIENTS = dict(
    ## these strings deinfe the external API for selecting the kvlayer
    ## storage backends
    cassandra=CStorage,
    accumulo=AStorage,
    local=LocalStorage,
    filestorage=FileStorage,
)

if PGStorage:
    STORAGE_CLIENTS['postgres'] = PGStorage

def client(config):
    '''
    constructs a storage client for the storage_type specified in config
    '''
    return STORAGE_CLIENTS[config['storage_type']](config)

def add_arguments(parser, defaults=None, include_app_name=False, include_namespace=False):
    '''
    add command line arguments to an argparse.ArgumentParser instance.
    This provides sensible defaults and accurate help messages, so
    that libraries that use kvlayer can provide these flags in their
    command line interfaces.

    kvlayer does not provide any command line interface itself.
    '''
    if  defaults is None:
        defaults = dict()

    if include_app_name:
        parser.add_argument('--app-name', default=defaults.get('app_name'), 
                            help='name of app for namespace prefixing')
    if include_namespace:
        parser.add_argument('--namespace', default=None, help='namespace for prefixing table names')

    ## standard flags that are unique to kvlayer
    parser.add_argument('--storage-type', default='local', 
                        help='backend type for kvlayer, e.g. "local" or "accumulo"')
    parser.add_argument('--storage-addresses', action='append', default=[], 
                        help='network addresses for kvlayer, can be repeated')

    parser.add_argument('--connection-pool-size', default=2,
                        help='number of connections for kvlayer to open in advance')
    parser.add_argument('--max-consistency-delay', default=120,
                        help='number of seconds for kvlayer to wait for DB cluster sync')
    parser.add_argument('--replication-factor', default=1,
                        help='number of copies of the data for kvlayer to require of DB cluster')
    parser.add_argument('--thrift-framed-transport-size-in-mb', default=15,
                        help='must not exceed value set on the server-side of DB cluster.  15MB is hardcoded default in thrift.')
