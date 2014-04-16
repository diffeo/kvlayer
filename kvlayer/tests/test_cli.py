'''
applications using kvlayer can call kvlayer.add_arguments(parser) to
get sensible defaults and help messages for command line options.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from cStringIO import StringIO
import logging
import uuid

import pytest

import kvlayer
from kvlayer._client import Actions
import yakonfig

logger = logging.getLogger(__name__)

@pytest.yield_fixture
def actions(namespace_string):
    with yakonfig.defaulted_config([kvlayer], params={
            'app_name': 'diffeo',
            'namespace': namespace_string,
            'storage_type': 'local',
            'storage_addresses': [],
    }):
        a = Actions(stdout=StringIO())
        yield a
        a.client.delete_namespace()

@pytest.fixture
def a_key(actions):
    client = actions.client
    client.setup_namespace(dict(t1=1))
    k1 = (uuid.uuid4(),)
    client.put('t1', (k1, 'some data'))
    return k1

def test_delete(actions, a_key):
    assert list(actions.client.get('t1', a_key)) == [(a_key, 'some data')]
    actions.runcmd('delete', ['-y'])
    assert actions.stdout.getvalue().startswith("deleting namespace '")
    assert list(actions.client.get('t1', a_key)) == [(a_key, None)]

def test_keys(actions, a_key):
    actions.runcmd('keys', ['t1', '1'])
    assert actions.stdout.getvalue() == repr(a_key) + '\n'

def test_keys_none(actions):
    actions.runcmd('keys', ['t1', '1'])
    assert actions.stdout.getvalue() == ''

def test_keys_by_uuid(actions, a_key):
    actions.runcmd('keys', ['t1', 'uuid'])
    assert actions.stdout.getvalue() == repr(a_key) + '\n'

def test_get(actions, a_key):
    actions.runcmd('get', ['t1', '1', a_key[0].hex])
    assert actions.stdout.getvalue() == "'some data'\n"

def test_get_none(actions, a_key):
    b_key = uuid.UUID(int=a_key[0].int + 1)
    actions.runcmd('get', ['t1', '1', b_key.hex])
    assert (actions.stdout.getvalue() ==
            'No values for key (' + repr(b_key) + ',).\n')
