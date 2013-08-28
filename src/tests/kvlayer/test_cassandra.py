import os
import time
import yaml
import uuid
import pytest
import kvlayer
from kvlayer import MissingID
from kvlayer._local_memory import LocalStorage
from kvlayer._cassandra import CStorage

from _setup_logging import logger

from make_namespace_string import make_namespace_string
namespace = make_namespace_string()

def test_local_storage_singleton():
    config_local = dict(
        storage_type='local',
        )
    local_storage = LocalStorage(config_local)
    local_storage.setup_namespace(namespace, dict(meta=1))
    keys_and_values = ((uuid.uuid4(),), b'hi')
    local_storage.put('meta', keys_and_values)
    key_range = (keys_and_values[0], keys_and_values[0])
    meta = list(local_storage.get('meta', key_range))
    assert meta[0][1] == b'hi'
    local_storage2 = LocalStorage(config_local)
    meta = list(local_storage2.get('meta', key_range))
    assert meta[0][1] == b'hi'

config_path = os.path.join(os.path.dirname(__file__), 'config_cassandra.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))


@pytest.fixture(scope="module", params=[
    ('local', ''),
    ('cassandra', 'test-cassandra-1.diffeo.com'),
])
def client(request):

    global config
    config['storage_type'] = request.param[0]
    config['storage_addresses'] = [request.param[1]]

    client = kvlayer.client(config)

    client.delete_namespace(namespace)

    def fin():
        client.delete_namespace(namespace)
        logger.info('tearing down %r' % namespace)
    request.addfinalizer(fin)

    return client


def test_low_level_storage(client):
    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2, t2=3))
    # use time-based UUID 1, so these are ordered
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    client.put('t2', ((u1, u2, u3), b'88'))
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    with pytest.raises(MissingID):
        list(client.get('t2', ((u2,), (u3,))))


@pytest.mark.performance
def test_low_level_large_writes(client):
    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2, t2=3))

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


def test_low_level_storage_setup_namespace_idempotent(client):
    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2))
    u1, u2, u3 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
    client.put('t1', ((u1, u2), b'88'))
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    client.setup_namespace(namespace, dict(t1=2))
    assert 1 == len(list(client.get('t1')))
    assert 1 == len(list(client.get('t1', ((u1,), (u1,)))))

    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2))
    assert 0 == len(list(client.get('t1')))
    with pytest.raises(MissingID):
        list(client.get('t1', ((u1,), (u1,))))


def test_low_level_storage_speed(client):
    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2, t2=3))
    num_rows = 10 ** 4
    t1 = time.time()
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4(),
                         uuid.uuid4(), uuid.uuid4()), b'')
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


def test_low_level_clear_table(client):
    client.delete_namespace(namespace)
    client.setup_namespace(namespace, dict(t1=2, t2=3))
    num_rows = 10 ** 2
    # make two tables and reset only one
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4(),
                        uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    client.put('t2', *[((uuid.uuid4(), uuid.uuid4(),
                         uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    client.clear_table('t1')
    assert len(list(client.get('t1'))) == 0
    assert len(list(client.get('t2'))) == num_rows

    client.put('t1', *[((uuid.uuid4(), uuid.uuid4(),
                         uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    assert len(list(client.get('t1'))) == num_rows
