import os
import sys
import time
import yaml
import uuid
import pytest
import kvlayer
from kvlayer import MissingID, BadKey
from kvlayer._local_memory import LocalStorage
from tempfile import NamedTemporaryFile

from _setup_logging import logger

from make_namespace_string import make_namespace_string

from _setup_logging import logger

config_local = dict(
    storage_type='local',
    ## LocalStorage does not need namespace
    )

tempfile = NamedTemporaryFile(delete=True)
new_tempfile = NamedTemporaryFile(delete=True)

config_file= dict(
    filename = tempfile.name,
    copy_to_filename =new_tempfile.name
    )

config_path = os.path.join(os.path.dirname(__file__), 'config_cassandra.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config_cassandra = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))

config_path = os.path.join(os.path.dirname(__file__), 'config_accumulo.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config_accumulo = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))


config_postgres = {
    'namespace': None,  # doesn't matter, gets clobbered below
    'storage_addresses': None,  # doesn't matter, gets clobbered below
}


@pytest.fixture(scope='function', params=[
    ('local', '', 'config_local'),
    ('filestorage', '', 'config_file'),
    ('cassandra', 'test-cassandra-1.diffeo.com', 'config_cassandra'),
    ('accumulo', 'test-accumulo-1.diffeo.com', 'config_accumulo'),
    ('postgres', 'host=test-postgres.diffeo.com port=5432 user=test dbname=test password=test', 'config_postgres'),
])
def client(request):
    config = globals()[request.param[2]]
    namespace = make_namespace_string()
    config['namespace'] = namespace
    config['app_name'] = 'kvlayer'
    logger.info('config: %r' % config)
    config['storage_type'] = request.param[0]
    config['storage_addresses'] = [request.param[1]]

    client = kvlayer.client(config)
    client.delete_namespace()

    def fin():
        client.delete_namespace()
        logger.info('tearing down %r_%r', config['app_name'], namespace)
    request.addfinalizer(fin)

    return client


def test_basic_storage(client):
    client.setup_namespace(dict(t1=2, t2=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    client.put('t2', ((u1, u2, u3), b'88'))
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    with pytest.raises(MissingID):
        list(client.get('t2', ((u2,), (u3,))))

def test_adding_tables(client):
    client.setup_namespace(dict(t1=2, t2=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'11'))
    client.put('t2', ((u1, u2, u3), b'22'))

    client.setup_namespace(dict(t3=1))
    client.put('t3', ((u1,), b'33'))

    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))
    assert 1 == len(list(client.get('t3', ((u1,), (u1,)))))

    with pytest.raises(MissingID):
        list(client.get('t2', ((u2,), (u3,))))

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
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    client.setup_namespace(dict(t1=2))
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    client.delete_namespace()
    client.setup_namespace(dict(t1=2))
    assert 0 == len(list(client.get('t1')))
    with pytest.raises(MissingID):
        list(client.get('t1', ((u1,), (u1,))))


def test_storage_speed(client):
    client.setup_namespace(dict(t1=2, t2=3))
    num_rows = 10 ** 4
    t1 = time.time()
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    t2 = time.time()
    results = list(client.get('t1', batch_size=num_rows))
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
    assert len(list(client.get('t1'))) == 0
    assert len(list(client.get('t2'))) == num_rows

    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    assert len(list(client.get('t1'))) == num_rows


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
