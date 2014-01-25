

import getpass
import hashlib
import logging
import os
import pytest
import socket

import kvlayer
import yakonfig

from _setup_logging import logger

def make_namespace_string(test_name='test'):
    '''
    generates a descriptive namespace for testing, which is unique to
    this user and also this process ID and host running the test.

    This also ensures that the namespace name is shorter than 48 chars
    and obeys the other constraints of the various backend DBs that we
    use.
    '''
    return '_'.join([
            test_name[:25], 
            getpass.getuser().replace('-', '_')[:5],
            str(os.getpid()),
            hashlib.md5(socket.gethostname()).hexdigest()[:4],
            ])

@pytest.fixture(scope='function')
def namespace(request):
    
    namespace = make_namespace_string()

    def fin():
        logger.info('tearing down %s...', namespace)
        try:
            config = yakonfig.get_global_config('kvlayer')
            ## this is probably already in the config
            config['namespace'] = namespace
            client = kvlayer.client(config)
            client.delete_namespace()
            logger.info('finished tearing down %s.', namespace)
        except KeyError:
            logger.critical('%s not configured in this process; cannot guess config', namespace)
        except Exception, exc:
            logger.critical('failed to tear down %s', namespace, exc_info=True)

    request.addfinalizer(fin)

    return namespace
