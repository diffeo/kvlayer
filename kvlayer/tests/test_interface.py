"""Basic functional tests for all of the kvlayer backends.

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.

"""

import errno
import logging
import os
import random
import sys
import time
import uuid

import py
import pytest
import yaml

import kvlayer
from kvlayer import MissingID, BadKey

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module',
                params=['local', 'filestorage', 'cassandra', 'accumulo',
                        'postgres', 'redis'])
def backend(request):
    return request.param

@pytest.fixture(scope='module')
def config(backend, request):
    config_path = request.fspath.new(basename='config_{}.yaml'.format(backend))
    try:
        with config_path.open('r') as f:
            return yaml.load(f)
    except py.error.ENOENT:
        return { 'storage_type': backend,
                 'namespace': None,
                 'storage_addresses': None }

@pytest.fixture(scope='function')
def client(config, request, tmpdir, _namespace_string):
    config['namespace'] = _namespace_string
    config['app_name'] = 'kvlayer'

    # this is hacky but must go somewhere
    if config['storage_type'] == 'filestorage':
        local = tmpdir.join('local')
        with local.open('w') as f: pass
        config['filename'] = str(local)

        copy = tmpdir.join('copy')
        config['copy_to_filename'] = str(copy)

    logger.info('config: %r' % config)

    client = kvlayer.client(config)
    client.delete_namespace()

    def fin():
        client.delete_namespace()
    request.addfinalizer(fin)

    return client


def test_basic_storage(client):
    client.setup_namespace(dict(t1=2, t2=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    client.put('t2', ((u1, u2, u3), b'88'))
    assert 1 == len(list(client.scan('t1')))
    assert 1 == len(list(client.scan('t1', ((u1,), (u1,)))))

    client.delete('t1', (u1, u2))
    assert 0 == len(list(client.scan('t1')))
    with pytest.raises(MissingID):
        list(client.scan('t1', ((u1,), (u1,))))

    with pytest.raises(MissingID):
        list(client.scan('t2', ((u2,), (u3,))))

def test_delete(client):
    client.setup_namespace({'table1': 1})
    kv_dict = {(uuid.uuid4(),): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    client.put('table1', *keys_and_values)
    keys = kv_dict.keys()
    delete_keys = keys[0:len(keys):2]
    save_keys = keys[1:len(keys):2]
    assert set(delete_keys)
    client.delete('table1', *delete_keys)
    for key in save_keys:
        for key, value in client.get('table1', key):
            assert kv_dict[key] == value
    for key in delete_keys:
        generator = client.get('table1', key)
        with pytest.raises(MissingID):
            row = generator.next()
            assert not row

def test_get(client):
    client.setup_namespace(dict(t1=1, t2=2, t3=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    client.put('t1', ((u1,), b'81'))
    res = list(client.get('t1', (u1, )))
    assert len(res) == 1
    assert res[0][0] == (u1,)
    assert res[0][1] == b'81'

    client.put('t2', ((u1, u2), b'82'))
    res = list(client.get('t2', (u1, u2)))
    assert len(res) == 1
    assert res[0][0] == (u1, u2)
    assert res[0][1] == b'82'

    client.put('t3', ((u1, u2, u3), b'83'))
    res = list(client.get('t3', (u1, u2, u3)))
    assert len(res) == 1
    assert res[0][0] == (u1, u2, u3)
    assert res[0][1] == b'83'



def test_adding_tables(client):
    client.setup_namespace(dict(t1=2, t2=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'11'))
    client.put('t2', ((u1, u2, u3), b'22'))

    client.setup_namespace(dict(t3=1))
    client.put('t3', ((u1,), b'33'))

    assert 1 == len(list(client.scan('t1')))
    assert 1 == len(list(client.scan('t1', ((u1,), (u1,)))))
    assert 1 == len(list(client.scan('t3', ((u1,), (u1,)))))

    with pytest.raises(MissingID):
        list(client.scan('t2', ((u2,), (u3,))))

@pytest.mark.performance
def test_large_writes(client):
    client.setup_namespace(dict(t1=2, t2=3))

    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    fifteen_mb_string = b' ' * 15 * 2 ** 20
    ## chop off the end to leave room for thrift message overhead
    long_string = fifteen_mb_string[: -256]

    num = 10

    for rows in xrange(num):
        logger.info('writing 1 string of length %d' % len(long_string))
        client.put('t1', ((u1, u2), long_string))

    logger.info('writing %d strings of length %d' % (num, len(long_string)))
    client.put('t1', *(((u1, u2), long_string) for row in xrange(num)))


def test_setup_namespace_idempotent(client):
    client.setup_namespace(dict(t1=2))
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    assert 1 == len(list(client.scan('t1')))
    assert 1 == len(list(client.scan('t1', ((u1,), (u1,)))))

    client.setup_namespace(dict(t1=2))
    assert 1 == len(list(client.scan('t1')))
    assert 1 == len(list(client.scan('t1', ((u1,), (u1,)))))

    client.delete_namespace()
    client.setup_namespace(dict(t1=2))
    assert 0 == len(list(client.scan('t1')))
    with pytest.raises(MissingID):
        list(client.scan('t1', ((u1,), (u1,))))


def test_storage_speed(client):
    client.setup_namespace(dict(t1=2, t2=3))
    num_rows = 10 ** 4
    t1 = time.time()
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    t2 = time.time()
    results = list(client.scan('t1', batch_size=num_rows))
    t3 = time.time()
    assert num_rows == len(results)
    put_rate = float(num_rows) / (t2 - t1)
    get_rate = float(num_rows) / (t3 - t2)
    logger.info('%d rows put=%.1f sec (%.1f per sec) '
                'get=%.1f sec (%.1f per sec)' % (
                num_rows, (t2 - t1), put_rate, (t3 - t2), get_rate))


def test_clear_table(client):
    client.setup_namespace(dict(t1=2, t2=3))
    num_rows = 10 ** 2
    # make two tables and reset only one
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    client.put('t2', *[((uuid.uuid4(), uuid.uuid4(),
                         uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    client.clear_table('t1')
    assert len(list(client.scan('t1'))) == 0
    assert len(list(client.scan('t2'))) == num_rows

    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    assert len(list(client.scan('t1'))) == num_rows


def test_bogus_put(client):
    client.setup_namespace(dict(t1=2, t2=3))
    num_rows = 10 ** 2
    # make two tables and reset only one
    with pytest.raises(BadKey):
        client.put('t1', *[((uuid.uuid4(), uuid.uuid4(),
                             uuid.uuid4(), uuid.uuid4()), b'')
                           for i in xrange(num_rows)])
    with pytest.raises(BadKey):
        client.put('t2', *[((uuid.uuid4(), uuid.uuid4(),
                             uuid.uuid4(), uuid.uuid4()), b'')
                           for i in xrange(num_rows)])


def sequence_to_one(iterable):
    val = None
    for tv in iterable:
        if val is None:
            val = tv
        else:
            raise Exception('expected one but got more than one value from %r' % (iterable,))
    if val is None:
        raise Exception('epected one value but got nothing from %r' % (iterable,))
    return val


def test_binary_clean(client):
    '''Test binary integrity moving non-text bytes to db and back.'''
    client.setup_namespace(dict(t1=2))
    keya = (uuid.uuid4(), uuid.uuid4())
    # every byte value from 0..255
    vala = bytes(b''.join([chr(x) for x in xrange(0,256)]))
    client.put('t1', (keya, vala))
    keyb = (uuid.uuid4(), uuid.uuid4())
    valb = bytes(b''.join([chr(random.randint(0,255)) for x in xrange(1000)]))
    client.put('t1', (keyb, valb))
    xvala = sequence_to_one(client.scan('t1', (keya, keya)))
    assert xvala[1] == vala
    xvalb = sequence_to_one(client.scan('t1', (keyb, keyb)))
    assert xvalb[1] == valb

def test_scan(client):
    client.setup_namespace(dict(t1=2))

    ## Add all keys and values from 0 to 90 counting by 10s.
    for x in xrange(0,100,10):
        key = (uuid.UUID(int=x), uuid.UUID(int=x))
        client.put('t1', (key, '%d' % x))

    ## Scan entire range
    expected_results = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    actual_results = [int(v) for k,v in client.scan('t1', ((), ()))]
    assert actual_results == expected_results

    ## Scan with specified start and stop values
    expected_results = [40, 50, 60, 70]
    actual_results = [int(v) for k,v in
                      client.scan('t1', ((uuid.UUID(int=40),),
                                         (uuid.UUID(int=75),)))]
    assert actual_results == expected_results

    ## Scan to unspecified end of range from start key
    expected_results = [80, 90]
    actual_results = [int(v) for k,v in
                      client.scan('t1', ((uuid.UUID(int=80),),()))]
    assert actual_results == expected_results

    ## Scan from minimum value to specified middle value
    expected_results = [0, 10]
    actual_results = [int(v) for k,v in
                      client.scan('t1', ((uuid.UUID(int=0),),
                                         (uuid.UUID(int=15),)))]
    assert actual_results == expected_results

    ## Scan from unspecified start value to middle value
    expected_results = [0, 10, 20, 30, 40, 50, 60, 70]
    actual_results = [int(v) for k,v in
                      client.scan('t1', ((),(uuid.UUID(int=75),)))]
    assert actual_results == expected_results

def test_no_keys(client):
    """Test that standard kvlayer APIs work correctly when not passed keys"""
    client.setup_namespace({'t1': 1})
    u = (uuid.uuid4(),)
    # this is a bug in several backend responses but we'll accept it for now
    uu = [u[0]]
    client.put('t1', (u, 'value'))
    assert list(client.get('t1', u)) == [(u, 'value')]
    assert list(client.scan('t1')) in [ [(u, 'value')], [(uu, 'value')] ]

    client.delete('t1')
    assert list(client.scan('t1')) in [ [(u, 'value')], [(uu, 'value')] ]

    client.get('t1')
    assert list(client.scan('t1')) in [ [(u, 'value')], [(uu, 'value')] ]

    client.put('t1')
    assert list(client.scan('t1')) in [ [(u, 'value')], [(uu, 'value')] ]
