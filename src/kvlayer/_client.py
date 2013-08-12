'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

from kvlayer._accumulo import AStorage
from kvlayer._cassandra import CStorage
from kvlayer._local_memory import LocalStorage

STORAGE_CLIENTS = dict(
    ## these strings deinfe the external API for selecting the kvlayer
    ## storage backends
    cassandra=CStorage,
    accumulo=AStorage,
    local=LocalStorage,
)

def client(config):
    '''
    constructs a storage client for the storage_type specified in config
    '''
    return STORAGE_CLIENTS[config['storage_type']](config)
