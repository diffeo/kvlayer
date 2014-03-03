'''
applications using kvlayer can call kvlayer.add_arguments(parser) to
get sensible defaults and help messages for command line options.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
import argparse
from cStringIO import StringIO
import logging
import subprocess
import sys
import uuid

import kvlayer
import yakonfig

logger = logging.getLogger(__name__)

def test_config(namespace_string):
    config_yaml = '''
kvlayer:
  app_name: streamcorpus_pipeline
  namespace: %s
  storage_type: local
  storage_addresses: []
''' % namespace_string
    with yakonfig.defaulted_config([kvlayer], yaml=config_yaml) as config:
        assert config['kvlayer'] == yakonfig.get_global_config('kvlayer')
        assert config['kvlayer']['app_name'] == 'streamcorpus_pipeline'

        check_that_config_works()


def check_that_config_works():
    client = kvlayer.client()
    client.setup_namespace(dict(t1=1))
    k1 = (uuid.uuid4(),)
    client.put('t1', (k1, 'some data'))
    assert list(client.get('t1', k1))[0][1] == 'some data'


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('foo')
        args = yakonfig.parse_args(parser, [kvlayer])

        config = yakonfig.get_global_config('kvlayer')
        assert config['app_name'] == 'streamcorpus_pipeline'

        check_that_config_works()

        logger.critical('finished fake_app, now exiting')

    except Exception, exc:
        logger.critical('fake_app failed!', exc_info=True)
        raise


def test_fake_app(namespace_string):
    '''
    test pretends to be an app using kvlayer.add_arguments
    '''
    p = subprocess.Popen(
        ['python', '-m', 'kvlayer.tests.test_cli', 
         'foo',
         '--app-name', 'streamcorpus_pipeline',
         '--namespace', namespace_string,
         '--storage-type', 'local',
         ])
    p.communicate()
    assert p.returncode == 0

if __name__ == '__main__':
    ## this is part of  test_fake_app
    main()
