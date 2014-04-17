"""Configuration parameters for kvlayer.

These collectively implement the `yakonfig.Configurable` interface.

-----

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.

"""
from __future__ import absolute_import

from kvlayer._client import STORAGE_CLIENTS
from yakonfig import ConfigurationError

config_name = 'kvlayer'
default_config = dict(
    storage_type = 'local',
    connection_pool_size = 2,
    max_consistency_delay = 120,
    replication_factor = 1,
    thrift_framed_transport_size_in_mb = 15,
    )

def add_arguments(parser):
    '''Add command line arguments to an argparse.ArgumentParser instance.'''
    parser.add_argument('--app-name',
                        help='name of app for namespace prefixing')
    parser.add_argument('--namespace',
                        help='namespace for prefixing table names')
    parser.add_argument('--storage-type',
                        help='backend type for kvlayer, e.g. "local" or "accumulo"')
    parser.add_argument('--storage-address', action='append',
                        dest='storage_addresses', metavar='HOST:PORT',
                        help='network addresses for kvlayer, can be repeated')
    parser.add_argument('--username', help='username for kvlayer accumulo')
    parser.add_argument('--password', help='password for kvlayer accumulo')

runtime_keys = dict(
    app_name = 'app_name',
    namespace = 'namespace',
    username = 'username',
    password = 'password',
    storage_type = 'storage_type',
    storage_addresses = 'storage_addresses',
    connection_pool_size = 'connection_pool_size',
    max_consistency_delay = 'max_consistency_delay',
    replication_factor = 'replication_factor',
    thrift_framed_transport_size_in_mb = 'thrift_framed_transport_size_in_mb',

    # these support testing and aren't exposed as command-line arguments
    kvlayer_filename = 'filename',
    kvlayer_copy_to_filename = 'copy_to_filename',
)

def check_config(config, name):
    if 'storage_type' not in config:
        raise ConfigurationError('{} must have a storage_type'
                                 .format(name))
    if config['storage_type'] not in STORAGE_CLIENTS:
        raise ConfigurationError('invalid {} storage_type {}'
                                 .format(name, config['storage_type']))
    if 'namespace' not in config:
        raise ConfigurationError('{} requires a namespace'.format(name))
    if 'app_name' not in config:
        raise ConfigurationError('{} requires an app_name'.format(name))
