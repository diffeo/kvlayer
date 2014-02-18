import logging

import pytest

import kvlayer
import yakonfig

logger = logging.getLogger(__name__)

@pytest.fixture(scope='function')
def namespace(request, namespace_string):
    def fin():
        logger.info('tearing down %s...', namespace_string)
        try:
            config = yakonfig.get_global_config('kvlayer')
            ## this is probably already in the config
            config['namespace'] = namespace_string
            client = kvlayer.client(config)
            client.delete_namespace()
            logger.info('finished tearing down %s.', namespace_string)
        except KeyError:
            logger.warn('%s not configured in this process; cannot guess config', namespace_string)
        except Exception, exc:
            logger.error('failed to tear down %s', namespace_string, exc_info=True)
    request.addfinalizer(fin)
    return namespace_string
