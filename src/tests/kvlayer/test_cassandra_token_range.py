import os
import sys
import cql
import yaml
import time
import uuid
import random
import pytest
import kvlayer
import getpass
import pycassa
import logging
import itertools
from operator import attrgetter
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
    LEXICAL_UUID_TYPE, ASCII_TYPE, BYTES_TYPE
from pycassa.types import CompositeType, TimeUUIDType, LexicalUUIDType, UUIDType, UTF8Type
from make_namespace import make_namespace_string

from _setup_logging import logger

one_mb = ' ' * 2**20

@pytest.fixture(scope='function')
def client(request):
    config_path = os.path.join(os.path.dirname(__file__), 'config_cassandra.yaml')
    if not os.path.exists(config_path):
        sys.exit('failed to find %r' % config_path)
    try:
        config = yaml.load(open(config_path))
    except Exception, exc:
        sys.exit('failed to load %r: %s' % (config_path, exc))
    namespace = make_namespace_string('cql_token_range')
    config['namespace'] = namespace
    config['app_name'] = 'kvlayer_tests'
    logger.info('config: %r' % config)
    client = kvlayer.client(config)
    def fin():
        client.delete_namespace()
        logger.info('tearing down %s_%s', config['app_name'], namespace)
    request.addfinalizer(fin)
    return client

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def test_composite_column_names(client):
    '''
    examine the unique nature of cassandras "wide sorted rows";

    Basically, C* is good at storing large OrderedDict objects and
    less good at the simpler kind of key=value storage that we were
    doing earlier.
    '''
    config = client._config
    namespace = client._app_namespace
    chosen_server = client._chosen_server
    sm = SystemManager(chosen_server)
    sm.create_keyspace(namespace, SIMPLE_STRATEGY, {'replication_factor': '1'})

    family = 'test'
    sm.create_column_family(
        namespace, family, super=False,
        key_validation_class = ASCII_TYPE,
        default_validation_class = BYTES_TYPE,
        comparator_type=CompositeType(UUIDType(), UUIDType()),
        #column_name_class = LEXICAL_UUID_TYPE
        )

    logger.info( sm.describe_schema_versions() )

    pool = ConnectionPool(namespace, config['storage_addresses'],
                          max_retries=1000, pool_timeout=10, pool_size=2, timeout=120)

    cf = pycassa.ColumnFamily(pool, family)
    u1, u2, u3, u4 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    ## insert four
    cf.insert('inbound', {(u1, u2): b''})
    cf.insert('inbound', {(u1, u3): b''})
    cf.insert('inbound', {(u1, u4): b''})
    cf.insert('inbound', {(u1, u1): b'45'})

    ## insert three with duplicates
    cf.insert('inbound', {(u3, u3): b'42'})
    cf.insert('inbound', {(u3, u3): b'43'})
    cf.insert('inbound', {(u3, u2): b''})
    cf.insert('inbound', {(u3, u4): b''})

    ## insert two
    cf.insert('inbound', {(u2, u3): b''})
    cf.insert('inbound', {(u2, u4): b''})

    ## verify start/finish parameters work as expected
    assert 4 == len(cf.get('inbound', column_start=(u1,), column_finish=(u1,)))
    assert 2 == len(cf.get('inbound', column_start=(u2,), column_finish=(u2,)))
    assert 3 == len(cf.get('inbound', column_start=(u3,), column_finish=(u3,)))

    ## check delete of a single column
    cf.remove('inbound', columns=[(u2, u3)])
    assert 1 == len(cf.get('inbound', column_start=(u2,), column_finish=(u2,)))

    ## check that the last inserted value appears at the expected
    ## location in the OrderedDict
    assert '43' == cf.get('inbound', column_start=(u3,), column_finish=(u3,)).items()[1][1]

    rec1 = cf.get('inbound', column_start=(u3,u3), column_finish=(u3,u3)).items()
    assert len(rec1) == 1

    start  = uuid.UUID(int=0)
    finish = uuid.UUID(int=2**128-1)
    assert start < u1 < u2 < u3 < u4 < finish
    assert 8 == len(cf.get('inbound', column_start=(start,), column_finish=(finish,)).items())

    ## test range searching
    start  = uuid.UUID(int=u3.int - 1)
    finish = uuid.UUID(int=u3.int + 1)
    assert start.int < u3.int < finish.int
    rec2 = cf.get('inbound', column_start=(start,), column_finish=(finish,)).items()
    assert rec2[1][0] == rec1[0][0]
    assert rec2[1][1] == rec1[0][1] == '43'

    cf.insert('inbound', {(u3,u3): b''.join(map(lambda u: u.bytes, (u1,u2,u3,u4)))})
    data = cf.get('inbound', column_start=(u3,u3), column_finish=(u3,u3)).items()[0][1]

    assert [u1,u2,u3,u4] == map(lambda b: uuid.UUID(bytes=b), grouper(data, 16))

    sm.close()

def test_composite_column_names_with_decomposited_keys(client):
    '''
    examine the unique nature of cassandras "wide sorted rows" using
    concatenated keys
    '''
    config = client._config
    namespace = client._app_namespace
    chosen_server = client._chosen_server
    sm = SystemManager(chosen_server)
    sm.create_keyspace(namespace, SIMPLE_STRATEGY, {'replication_factor': '1'})

    family = 'test'
    sm.create_column_family(
        namespace, family, super=False,
        key_validation_class = ASCII_TYPE,
        default_validation_class = BYTES_TYPE,
        comparator_type=UTF8Type()
        )

    logger.info( sm.describe_schema_versions() )

    pool = ConnectionPool(namespace, config['storage_addresses'],
                          max_retries=1000, pool_timeout=10, pool_size=2, timeout=120)

    cf = pycassa.ColumnFamily(pool, family)
    u1, u2, u3, u4 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    ## insert four
    cf.insert('inbound', {join_uuids(u1, u2): b''})
    cf.insert('inbound', {join_uuids(u1, u3): b''})
    cf.insert('inbound', {join_uuids(u1, u4): b''})
    cf.insert('inbound', {join_uuids(u1, u1): b'45'})

    ## insert three with duplicates
    cf.insert('inbound', {join_uuids(u3, u3): b'42'})
    cf.insert('inbound', {join_uuids(u3, u3): b'43'})
    cf.insert('inbound', {join_uuids(u3, u2): b''})
    cf.insert('inbound', {join_uuids(u3, u4): b''})

    ## insert two
    cf.insert('inbound', {join_uuids(u2, u3): b''})
    cf.insert('inbound', {join_uuids(u2, u4): b''})

    ## verify start/finish parameters work as expected
    assert 4 == len(cf.get('inbound', column_start=join_uuids(u1, num_uuids=2), column_finish=join_uuids(u1, num_uuids=2, padding='f')))
    assert 2 == len(cf.get('inbound', column_start=join_uuids(u2, num_uuids=2), column_finish=join_uuids(u2, num_uuids=2, padding='f')))
    assert 3 == len(cf.get('inbound', column_start=join_uuids(u3, num_uuids=2), column_finish=join_uuids(u3, num_uuids=2, padding='f')))

    ## check delete of a single column
    cf.remove('inbound', columns=[join_uuids(u2, u3)])
    assert 1 == len(cf.get('inbound', column_start=join_uuids(u2, num_uuids=2), column_finish=join_uuids(u2, num_uuids=2, padding='f')))

    ## check that the last inserted value appears at the expected
    ## location in the OrderedDict
    assert '43' == cf.get('inbound', column_start=join_uuids(u3, num_uuids=2), column_finish=join_uuids(u3, num_uuids=2, padding='f')).items()[1][1]

    rec1 = cf.get('inbound', column_start=join_uuids(u3,u3), column_finish=join_uuids(u3,u3)).items()
    assert len(rec1) == 1

    start  = uuid.UUID(int=0)
    finish = uuid.UUID(int=2**128-1)
    assert start < u1 < u2 < u3 < u4 < finish
    assert 8 == len(cf.get('inbound', column_start=join_uuids(start, num_uuids=2), column_finish=join_uuids(finish, num_uuids=2, padding='f')).items())

    ## test range searching
    start  = uuid.UUID(int=u3.int - 1)
    finish = uuid.UUID(int=u3.int + 1)
    assert start.int < u3.int < finish.int
    rec2 = cf.get('inbound', column_start=join_uuids(start, num_uuids=2), column_finish=join_uuids(finish, num_uuids=2, padding='f')).items()
    assert rec2[1][0] == rec1[0][0]
    assert rec2[1][1] == rec1[0][1] == '43'

    cf.insert('inbound', {join_uuids(u3,u3): b''.join(map(lambda u: u.bytes, (u1,u2,u3,u4)))})
    data = cf.get('inbound', column_start=join_uuids(u3,u3), column_finish=join_uuids(u3,u3)).items()[0][1]

    assert [u1,u2,u3,u4] == map(lambda b: uuid.UUID(bytes=b), grouper(data, 16))

    sm.close()

@pytest.mark.xfail
def test_composite_column_names_second_level_range_query(client):
    '''
    check that we can execute range queries on the second part of a
    CompositeType column name
    '''
    config = client._config
    namespace = client._app_namespace
    chosen_server = client._chosen_server
    sm = SystemManager(chosen_server)
    sm.create_keyspace(namespace, SIMPLE_STRATEGY, {'replication_factor': '1'})

    family = 'test'
    sm.create_column_family(
        namespace, family, super=False,
        key_validation_class = ASCII_TYPE,
        default_validation_class = BYTES_TYPE,
        comparator_type=CompositeType(UUIDType(), UUIDType()),
        )
    pool = ConnectionPool(namespace, config['storage_addresses'],
                          max_retries=1000, pool_timeout=10, pool_size=2, timeout=120)

    cf = pycassa.ColumnFamily(pool, family)
    u1, u2, u3, u4 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    cf.insert('inbound', {(u1, u2): b''})
    cf.insert('inbound', {(u1, u3): b''})
    cf.insert('inbound', {(u1, u4): b''})

    ## test range searching
    start  = uuid.UUID(int=u3.int - 1)
    finish = uuid.UUID(int=u3.int + 1)
    assert start.int < u3.int < finish.int
    rec3 = cf.get('inbound',
                  column_start =(u1, start),
                  column_finish=(u1, finish)).items()
    assert len(rec3) == 1
    assert rec3[0][0][1] == u3
    ####  This assert above passes!

    ####  This next part fails :-/
    ## now insert many rows -- enough that some should fall in each
    ## subrange below
    for i in xrange(1000):
        cf.insert('inbound', {(u1, uuid.uuid4()): b''})

    ## do four ranges, and expect more than zero in each
    step_size = 2**(128 - 2)
    for i in range(2**2, 0, -1):
        start =  uuid.UUID(int=(i-1) * step_size)
        finish = uuid.UUID(int=min(i * step_size, 2**128 - 1))
        recs = cf.get('inbound',
                      column_start =(u1, start),
                      column_finish=(u1, finish)).items()
        for key, val in recs:
            assert val == b''
            assert key[0] == u1
            assert key[1] < finish
            assert start < key[1]              ## this fails!!

        assert len(recs) > 0
        logger.info( '%r for %r %r' % (len(recs), start, finish))

    sm.close()

def join_uuids(*uuids, **kwargs):
    num_uuids = kwargs.pop('num_uuids', 0)
    padding = kwargs.pop('padding', '0')
    uuid_str = ''.join(map(attrgetter('hex'), uuids))
    uuid_str += padding * ((num_uuids * 32) - len(uuid_str))
    return uuid_str

def split_uuids(uuid_str):
    return map(lambda s: uuid.UUID(hex=''.join(s)), grouper(uuid_str, 32))

def test_composite_column_names_second_level_range_query_with_decomposited_keys(client):
    '''
    check that we can execute range queries on the second part of a
    CompositeType column name after we unpack the composite key into a
    long string of concatenated hex forms of the UUIDs
    '''
    config = client._config
    namespace = client._app_namespace
    chosen_server = client._chosen_server
    sm = SystemManager(chosen_server)
    sm.create_keyspace(namespace, SIMPLE_STRATEGY, {'replication_factor': '1'})

    family = 'test'
    sm.create_column_family(
        namespace, family, super=False,
        key_validation_class = ASCII_TYPE,
        default_validation_class = BYTES_TYPE,
        comparator_type=UTF8Type(),
        )
    pool = ConnectionPool(namespace, config['storage_addresses'],
                          max_retries=1000, pool_timeout=10, pool_size=2, timeout=120)

    cf = pycassa.ColumnFamily(pool, family)
    u1, u2, u3, u4 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), uuid.uuid1()

    cf.insert('inbound', {join_uuids(u1, u2): b''})
    cf.insert('inbound', {join_uuids(u1, u3): b''})
    cf.insert('inbound', {join_uuids(u1, u4): b''})

    ## test range searching
    start  = uuid.UUID(int=u3.int - 1)
    finish = uuid.UUID(int=u3.int + 1)
    assert start.int < u3.int < finish.int
    rec3 = cf.get('inbound',
                  column_start =join_uuids(u1, start),
                  column_finish=join_uuids(u1, finish)).items()
    assert len(rec3) == 1
    assert split_uuids(rec3[0][0])[1] == u3
    ####  This assert above passes!

    ####  This next part fails :-/
    ## now insert many rows -- enough that some should fall in each
    ## subrange below
    for i in xrange(1000):
        cf.insert('inbound', {join_uuids(u1, uuid.uuid4()): b''})

    ## do four ranges, and expect more than zero in each
    step_size = 2**(128 - 2)
    for i in range(2**2, 0, -1):
        start =  uuid.UUID(int=(i-1) * step_size)
        finish = uuid.UUID(int=min(i * step_size, 2**128 - 1))
        recs = cf.get('inbound',
                      column_start =join_uuids(u1, start),
                      column_finish=join_uuids(u1, finish)).items()
        for key, val in recs:
            key = split_uuids(key)
            assert val == b''
            assert key[0] == u1
            assert key[1] < finish
            assert start < key[1]   ## this passes!!

        assert len(recs) > 0
        logger.info( '%r for %r %r' % (len(recs), start, finish))

    sm.close()

def test_cql_paging(client):
    '''
    read rows from a range of tokens in Cassandra
    '''
    config = client._config
    server = config['storage_addresses'][0]
    server = server.split(':')[0]
    conn = cql.connect(server, cql_version='3.0.1')
    cursor = conn.cursor()

    try:
        cursor.execute('DROP KEYSPACE %s;' % client._app_namespace)
    except:
        pass

    ## make a keyspace and a simple table
    ## cursor.execute("CREATE KEYSPACE %s WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;" % client._app_namespace)
    cursor.execute("CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};" % client._app_namespace)
    cursor.execute("USE %s;" % client._app_namespace)
    cursor.execute('CREATE TABLE data (k int PRIMARY KEY, v varchar);')

    ## put some data in the table
    count = 0
    start_time = time.time()
    for idx in xrange(10):
        cursor.execute("INSERT INTO data (k, v) VALUES (%d, '%s');" % (idx, one_mb))
        count += 1
        if count % 10 == 0:
            elapsed = time.time() - start_time
            rate = float(count) / elapsed
            logger.info( '%d in %.1f sec --> %.1f row/sec' % (count, elapsed, rate) )

    cql3_command = "SELECT k, v FROM data limit 30000;"
    logger.info( cql3_command )

    cursor.execute(cql3_command)

    start = time.time()
    for row in cursor:
        logger.info( time.time() - start )
        start = time.time()

    #print "Number of results: ", len(list(cursor))

    ## remove this test keyspace
    try:
        cursor.execute('DROP KEYSPACE %s;' % client._app_namespace)
    except:
        pass

    cursor.close()
    conn.close()


def test_cql_token_range(client):
    '''
    read rows from a range of tokens in Cassandra
    '''
    config = client._config
    namespace = client._app_namespace
    chosen_server = client._chosen_server
    server = config['storage_addresses'][0]
    server = server.split(':')[0]
    conn = cql.connect(server, cql_version='3.0.1')
    cursor = conn.cursor()

    ## make a keyspace and a simple table
    ## cursor.execute("CREATE KEYSPACE %s WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1;" % client._app_namespace)
    try:
        ## remove this test keyspace
        cursor.execute('DROP KEYSPACE %s;' % client._app_namespace)
    except:
        pass
    cursor.execute("CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};" % client._app_namespace)
    cursor.execute("USE %s;" % client._app_namespace)
    cursor.execute('CREATE TABLE data (k int PRIMARY KEY, v varchar);')

    ## put some data in the table
    count = 0
    total_inbound = 100
    start_time = time.time()
    for idx in xrange(total_inbound):
        cursor.execute("INSERT INTO data (k, v) VALUES (%d, '%s');" % (idx, ' ')) #one_mb))
        count += 1
        if count % 10 == 0:
            elapsed = time.time() - start_time
            rate = float(count) / elapsed
            logger.info( '%d in %.1f sec --> %.1f row/sec' % (count, elapsed, rate) )

    assert count == total_inbound

    ## split up the full range of tokens.
    ## Suppose there are 2**k workers:
    k = 2 # --> four workers
    token_sub_range = 2**(64 - k)
    lowest_token = -1 * 2**63  ## for murmur3hash

    total_count = 0

    for worker_i in range(2**k):
        start_token = lowest_token +      worker_i  * token_sub_range
        end_token =   lowest_token + (1 + worker_i) * token_sub_range
        end_token = min(2**63 - 1, end_token)
        if worker_i == 2**k:
            ## catch the end
            pass#end_token += 1

        limit = 1000
        cql3_command = "SELECT k, v FROM data WHERE token(k) >= %d AND token(k) < %d LIMIT %d;" % (start_token, end_token, limit)
        logger.info( cql3_command )

        cursor.execute(cql3_command)

        start = time.time()
        for row in cursor:
            logger.info( time.time() - start )
            start = time.time()

            total_count += 1

    assert total_count == total_inbound

    ## remove this test keyspace
    cursor.execute('DROP KEYSPACE %s;' % client._app_namespace)

    cursor.close()
    conn.close()
