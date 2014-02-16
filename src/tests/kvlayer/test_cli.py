'''
applications using kvlayer can call kvlayer.add_arguments(parser) to
get sensible defaults and help messages for command line options.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
import argparse
from cStringIO import StringIO
import subprocess
import sys
import uuid

import kvlayer
import yakonfig

from tests.kvlayer.make_namespace import namespace
from tests.kvlayer._setup_logging import logger


def test_config(namespace):
    fh = StringIO('''
kvlayer:
  app_name: streamcorpus_pipeline
  namespace: %s
  storage_type: local
  storage_addresses: []
''' % namespace)
    config = yakonfig.set_global_config(fh)

    assert config['kvlayer'] == yakonfig.get_global_config('kvlayer')
    assert config['kvlayer']['app_name'] == 'streamcorpus_pipeline'

    check_that_config_works()


def check_that_config_works():
    config = yakonfig.get_global_config('kvlayer')
    client = kvlayer.client(config)
    client.setup_namespace(dict(t1=1))
    k1 = (uuid.uuid4(),)
    client.put('t1', (k1, 'some data'))
    assert list(client.get('t1', k1))[0][1] == 'some data'
    logger.critical('finished check_that_config_works: %r', config)


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('foo')
        ## add in the arguments provided by kvlayer
        kvlayer.add_arguments(parser, include_app_name=True, include_namespace=True)
        args = parser.parse_args()

        yakonfig.set_runtime_args_object(args)

        fh = StringIO(kvlayer.default_yaml())
        config = yakonfig.set_global_config(stream=fh)

        assert config['kvlayer'] == yakonfig.get_global_config('kvlayer')
        assert config['kvlayer']['app_name'] == 'streamcorpus_pipeline'

        check_that_config_works()

        logger.critical('finished fake_app, now exiting')

    except Exception, exc:
        logger.critical('fake_app failed!', exc_info=True)


def test_fake_app(namespace):
    '''
    test pretends to be an app using kvlayer.add_arguments
    '''
    p = subprocess.Popen(
        ['python', '-m', 'tests.kvlayer.test_cli', 
         'foo',
         '--app-name', 'streamcorpus_pipeline',
         '--namespace', namespace,
         '--storage-type', 'local',
         ], 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.critical('failure! p.returncode=%d', p.returncode)
        logger.critical(stderr)
        logger.critical(stdout)
        sys.exit(-1)


if __name__ == '__main__':
    ## this is part of  test_fake_app
    main()
