"""Basic functional tests for all of the kvlayer backends.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import logging
import random
from StringIO import StringIO  # cStringIO is not pickable
import uuid

import pkg_resources
import pytest
import yaml

from pytest_diffeo import redis_address
import kvlayer
from kvlayer import BadKey
from kvlayer._client import STORAGE_CLIENTS, load_entry_point_kvlayer_impls
import yakonfig

logger = logging.getLogger(__name__)


_extension_test_configs = {}

def load_entry_point_kvlayer_test_configs():
    global _extension_test_configs
    for entry_point in pkg_resources.iter_entry_points('kvlayer.test_config'):
        try:
            name = entry_point.name
            constructor = entry_point.load()
            _extension_test_configs[name] = constructor()
        except:
            logger.error('failed loading kvlayer test_config %r', entry_point and entry_point.name, exc_info=True)


# run at global scope to ensure it's before we might possibly need STORAGE_CLIENTS in any context
load_entry_point_kvlayer_impls()
load_entry_point_kvlayer_test_configs()


@pytest.fixture(scope='module',
                params=STORAGE_CLIENTS.keys())
def backend(request):
    backend = request.param
    if backend == 'cassandra':
        pytest.skip('cassandra doesn\'t support non-UUID keys')
    if backend in _extension_test_configs:
        pass # okay
    elif not request.fspath.dirpath('config_{}.yaml'.format(backend)).exists():
        pytest.skip('no configuration file for backend {}'.format(backend))
    if backend != 'local':
        pytest.skip('TODO DELETE TEMPROARY NONLOCAL SKIP')
    return backend

@pytest.yield_fixture(scope='function')
def client(backend, request, tmpdir, namespace_string):
    if backend in _extension_test_configs:
        file_config = yaml.load(_extension_test_configs[backend])
    else:
        config_path = str(request.fspath.dirpath('config_{}.yaml'.format(backend)))
        # read and parse the config file, insert an object
        with open(config_path, 'r') as f:
            file_config = yaml.load(f)

    # Insert an object into the config which stats will write to.
    # Below we can get the stats text and log it here.
    # (Normal stats flow logs to file.)
    file_config['kvlayer']['log_stats'] = StringIO()
    file_config['kvlayer']['encoder'] = 'packed'

    params = dict(
        app_name='kvlayer',
        namespace=namespace_string,
    )

    # this is hacky but must go somewhere
    if backend == 'filestorage':
        local = tmpdir.join('local')
        with local.open('w') as f: pass
        params['kvlayer_filename'] = str(local)

    if backend == 'redis':
        params['storage_addresses'] = [ redis_address(request) ]

    with yakonfig.defaulted_config(
            [kvlayer],
            config=file_config,
            params=params):
        client = kvlayer.client()
        client.delete_namespace()
        yield client
        if client._log_stats is not None:
            client._log_stats.flush()
            logger.info('storage stats (%s %s):\n%s',
                        backend, request.function.__name__,
                        file_config['kvlayer']['log_stats'].getvalue())
        client.delete_namespace()

def test_basic_storage(client):
    client.setup_namespace(dict(t1=2, t2=3))
    assert 0 == len(list(client.scan('t1')))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    client.put('t2', ((u1, u2, u3), b'88'))
    assert 1 == len(list(client.scan('t1')))
    assert 1 == len(list(client.scan('t1', ((u1,), (u1,)))))

    client.delete('t1', (u1, u2))
    assert 0 == len(list(client.scan('t1')))
    assert 0 == len(list(client.scan('t1', ((u1,), (u1,)))))
    assert 0 == len(list(client.scan('t2', ((u2,), (u3,)))))

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
        assert list(client.get('table1', key)) == [(key, None)]

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


def test_put_put(client):
    client.setup_namespace(dict(t1=1))
    u1 = uuid.uuid1()
    client.put('t1', ((u1,),'a'))
    client.put('t1', ((u1,),'b'))
    assert list(client.get('t1', (u1,))) == [((u1,),'b')]


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
    assert 0 == len(list(client.scan('t2', ((u2,), (u3,)))))


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
    assert 0 == len(list(client.scan('t1', ((u1,), (u1,)))))


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

    ## Scan with specified start and stop values
    ## 40 <= x <= 70
    expected_results = [40, 50, 60, 70]
    actual_results = [int(v) for k,v in
                      client.scan('t1', ((uuid.UUID(int=40),),
                                         (uuid.UUID(int=70),)))]
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

def test_scan_2d(client):
    '''Set up a table with keys x,y and scan a single known x.'''
    client.setup_namespace({'t1': 2})
    def kf(x, y): return (uuid.UUID(int=x), uuid.UUID(int=y))
    def k1f(x): return (uuid.UUID(int=x),)
    def vf(x, y): return '{}:{}'.format(x, y)
    def kvf(x, y): return (kf(x, y), vf(x, y))
    def scan(fr, to): return list(client.scan('t1', (fr, to)))
    r = range(1,5) # [1, 2, 3, 4]

    kvps = [kvf(x, y) for x in r for y in r]
    client.put('t1', *kvps)

    assert list(client.scan('t1')) == kvps
    assert scan(k1f(2), k1f(2)) == [kvf(2,y) for y in r]
    assert scan(kf(2,1), kf(2,2)) == [kvf(2,1), kvf(2,2)]
    assert scan(kf(2,0), kf(2,3)) == [kvf(2,1), kvf(2,2), kvf(2,3)]
    assert scan(kf(2,2), kf(2,5)) == [kvf(2,2), kvf(2,3), kvf(2,4)]
    assert scan(kf(2,5), kf(3,0)) == []
    assert scan(kf(2,4), kf(3,1)) == [kvf(2,4), kvf(3,1)]
    assert scan(k1f(4), k1f(6)) == [kvf(4,y) for y in r]
    assert scan(k1f(0), k1f(1)) == [kvf(1,y) for y in r]
    assert scan(k1f(0), k1f(6)) == kvps
    assert scan(kf(0,6), kf(6,0)) == kvps
    assert scan(k1f(0), k1f(0)) == []

def test_scan_keys_2d(client):
    '''Same as test_scan_2d, but only scan the key space.'''
    client.setup_namespace({'t1': 2})
    def kf(x, y): return (uuid.UUID(int=x), uuid.UUID(int=y))
    def k1f(x): return (uuid.UUID(int=x),)
    def vf(x, y): return '{}:{}'.format(x, y)
    def kvf(x, y): return (kf(x, y), vf(x, y))
    def scan_keys(fr, to): return list(client.scan_keys('t1', (fr, to)))
    r = range(1,5) # [1, 2, 3, 4]

    kvps = [kvf(x, y) for x in r for y in r]
    keys = [kf(x, y) for x in r for y in r]
    client.put('t1', *kvps)

    assert list(client.scan_keys('t1')) == keys
    assert scan_keys(k1f(2), k1f(2)) == [kf(2,y) for y in r]
    assert scan_keys(kf(2,1), kf(2,2)) == [kf(2,1), kf(2,2)]
    assert scan_keys(kf(2,0), kf(2,3)) == [kf(2,1), kf(2,2), kf(2,3)]
    assert scan_keys(kf(2,2), kf(2,5)) == [kf(2,2), kf(2,3), kf(2,4)]
    assert scan_keys(kf(2,5), kf(3,0)) == []
    assert scan_keys(kf(2,4), kf(3,1)) == [kf(2,4), kf(3,1)]
    assert scan_keys(k1f(4), k1f(6)) == [kf(4,y) for y in r]
    assert scan_keys(k1f(0), k1f(1)) == [kf(1,y) for y in r]
    assert scan_keys(k1f(0), k1f(6)) == keys
    assert scan_keys(kf(0,6), kf(6,0)) == keys
    assert scan_keys(k1f(0), k1f(0)) == []

def test_scan_2d_prefix(client):
    '''scan(('a',),('a',)) shouldn't find ('ab','c')'''
    client.setup_namespace({'ts2': (str,str)})
    client.put('ts2',
               (('a','a'),'1'),
               (('a','z'),'2'),
               (('ab','b'),'3'),
               (('ab','y'),'4'))

    assert (list(client.scan('ts2', (('a',),('a',)))) ==
            [(('a','a'),'1'),(('a','z'),'2')])  # but not anything ab
    assert (list(client.scan_keys('ts2', (('a',),('a',)))) ==
            [('a','a'),('a','z')])  # but not anything ab

def test_scan_9042(client):
    tname = 'ts9000'
    client.setup_namespace({tname: (str,)})

    kv = [
        (('k{:5x}'.format(x),), 'v{:5x}'.format(x))
        for x in xrange(9042)
    ]
    client.put(tname, *kv)

    # now check that scan gets them all back
    count = 0
    for rkv in client.scan(tname):
        count += 1

    assert count == 9042

    # try other ranged scans
    count = 0
    for rkv in client.scan(tname, (('k{:5x}'.format(3000),), ()) ):
        count += 1

    assert count == 6042

    count = 0
    for rkv in client.scan(tname, (('k{:5x}'.format(3000),), ('k{:5x}'.format(8437),)) ):
        count += 1

    assert count == 8437-3000+1 # +1 because scan is inclusive of endpoints

    count = 0
    for rkv in client.scan(tname, ((), ('k{:5x}'.format(8437),)) ):
        count += 1

    assert count == 8437+1 # +1 because scan is inclusive of endpoints


def test_scan_binary_key_order(client):
    client.setup_namespace({'s1':(str,int)})
    # keys in what should be sorted order.
    keys = [
        ('\0',1),
        ('\0\0',2),
        ('\0\x01',3),
        ('\x01',4),
        ('\x01\0', 5),
        ('\x01#aoeu', 6),
        ('\x02',7),
    ]
    values = ['{:05d}'.format(i) for i in xrange(1,len(keys)+1)]
    client.put(
        's1',
        *zip(keys, values)
    )

    d = list(client.scan('s1'))
    dvalues = [dx[1] for dx in d]
    # ensure that values come back out in expected order.
    assert dvalues == values

    d = list(client.scan('s1', (('\x01',), None)))
    assert len(d) == 4


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


def test_new_key_spec(client):
    """Test that new table key specs work in setup_namespace."""
    client.setup_namespace({
        'kt2': (str,int),
        'kt3': (uuid.UUID, (int,long)),
        'kt4': (str,str,str),
    })

    good_kt2_key = ('aoeu', 1337)
    client.put('kt2', (good_kt2_key, 'blah'))

    with pytest.raises(BadKey):
        client.put('kt2', ((147, 'aeou'), 'nope'))
    with pytest.raises(BadKey):
        client.put('kt2', ((uuid.uuid4(), 'aeou'), 'nope'))
    with pytest.raises(BadKey):
        client.put('kt2', (('foo', 'aeou'), 'nope'))
    with pytest.raises(BadKey):
        client.put('kt2', (('foo', 123987419234701239847120), 'nope'))

    kt3u = uuid.uuid4()
    good_kt3_kvs = [
        ((kt3u, 42), 'v1'),
        ((kt3u, 0x7fFFFFffffFFFF), 'v2'),
    ]
    client.put('kt3', *good_kt3_kvs)

    count = 0
    for k,v in client.scan('kt3', ((kt3u,), (kt3u,))):
        assert (k,v) in good_kt3_kvs
        count += 1
    assert count == len(good_kt3_kvs)

    good_kt4_kvs = [
        (('fooa', 'aoeu', 'snth'), 'v1'),
        (('foob', 'aoeu', 'snth'), 'v1'),
        (('fooc', 'aoeu', 'snth'), 'v1'),
    ]
    client.put('kt4', *good_kt4_kvs)
    count = 0
    for k,v in client.scan('kt4', (('foo',), ('fooz',))):
        assert (k,v) in good_kt4_kvs
        count += 1
    assert count == len(good_kt4_kvs)

def test_merge_join(client):
    '''Test that we can read two tables at the same time.'''
    client.setup_namespace({
        't1': (str,),
        't2': (str,),
    })
    client.put('t1',
               (('a',),'one'),
               (('b',),'two'),
               (('d',),'four'))
    client.put('t2',
               (('a',),'one'),
               (('c',),'three'))
    # Find keys in t1 not in t2
    iter1 = client.scan('t1')
    iter2 = client.scan('t2')
    missing = []
    v1 = next(iter1, None)
    v2 = next(iter2, None)
    while v1:
        if not v2:
            missing.append(v1)
            v1 = next(iter1, None)
        elif v1[0] < v2[0]:
            missing.append(v1)
            v1 = next(iter1, None)
        elif v1[0] == v2[0]:
            v1 = next(iter1, None)
            v2 = next(iter2, None)
        else:
            v2 = next(iter2, None)
    assert missing == [(('b',),'two'),(('d',),'four')]
        
def test_partial_scan(client):
    '''Test that reading part of a table, then starting a new scan,
    doesn't break.'''
    client.setup_namespace({'t1': (str,)})
    client.put('t1',
               (('a',),'one'),
               (('b',),'two'),
               (('c',),'three'),
               (('d',),'four'))

    s1 = client.scan('t1')
    assert next(s1, None) == (('a',),'one')
    assert next(s1, None) == (('b',),'two')

    s2 = client.scan('t1')
    assert next(s2, None) == (('a',),'one')
    assert next(s2, None) == (('b',),'two')
    assert next(s2, None) == (('c',),'three')
    assert next(s2, None) == (('d',),'four')
    assert next(s2, None) is None

def test_partial_scan_keys(client):
    '''Test that reading part of a table, then starting a new scan,
    doesn't break.'''
    client.setup_namespace({'t1': (str,)})
    client.put('t1',
               (('a',),'one'),
               (('b',),'two'),
               (('c',),'three'),
               (('d',),'four'))

    s1 = client.scan('t1')
    assert next(s1, None) == (('a',),'one')
    assert next(s1, None) == (('b',),'two')

    s2 = client.scan_keys('t1')
    assert next(s2, None) == ('a',)
    assert next(s2, None) == ('b',)
    assert next(s2, None) == ('c',)
    assert next(s2, None) == ('d',)
    assert next(s2, None) is None

def test_scan_name_oddity(client):
    client.setup_namespace({'index': (str,str,str)})
    row = (('NAME','alistair','vid'),'1')
    client.put('index', row)
    s = client.scan('index', (('NAME','a'), ('NAME','a\xff')))
    assert list(s) == [row]
    s = client.scan('index', (('NAME','al'), ('NAME','al\xff')))
    assert list(s) == [row]
    s = client.scan('index', (('NAME','ali'), ('NAME','ali\xff')))
    assert list(s) == [row]
    s = client.scan('index', (('NAME','alis'), ('NAME','alis\xff')))
    assert list(s) == [row]
    s = client.scan('index', (('NAME','alist'), ('NAME','alist\xff')))
    assert list(s) == [row]

def test_get_returns_keys(client):
    client.setup_namespace({'t': (str,)})
    key = ('fubar',)
    client.put('t', (key, '1'))
    results = list(client.get('t', key))
    assert len(results) == 1
    k, v = results[0]
    assert k == key
    assert v == '1'

def test_key_escaping(client):
    client.setup_namespace({'t': (str, str, str)})
    key = ('a\0b', 'a%b', 'a%00b')
    client.put('t', (key, '1'))
    val = list(client.get('t', key))
    assert len(val) == 1
    k, v = val[0]
    assert k == key
    assert v == '1'
