import os
import re
import sys
import uuid
import yaml
import pytest
import kvlayer

from pyaccumulo import Accumulo, Mutation

from _setup_logging import logger

from kvlayer._accumulo import AStorage
from make_namespace_string import make_namespace_string

namespace = make_namespace_string()

config_path = os.path.join(os.path.dirname(__file__), 'config_accumulo.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))


def ns(name):
    return name + '_' + namespace


def fin():
    conn = Accumulo(host=config['host'], port=50096,
                    user="root", password="diffeo")
    tables = conn.list_tables()
    for table in tables:
        if re.search(namespace, table):
            conn.delete_table(table)


@pytest.fixture
def direct(request):
    conn = Accumulo(host='test-accumulo-1.diffeo.com', port=50096,
                    user="root", password="diffeo")
    request.addfinalizer(fin)
    return conn

@pytest.fixture(scope='module', params=['accumulo'])
def client(request):

    global config
    config['storage_type'] = request.param

    logger.info('initializing client')
    client = kvlayer.client(config)

    logger.info('deleting old namespace')
    #client.delete_namespace(namespace)

    def fin():
        logger.info('tearing down %r' % namespace)
        client.delete_namespace(namespace)
        logger.info('done cleaning up')
    request.addfinalizer(fin)

    logger.info('starting test')
    return client


def test_init_accumulo():
    storage = AStorage(config)
    assert storage


def test_ns():
    storage = AStorage(config)
    ## default config sets namespace='test'
    assert storage._ns('test') == 'test_test'


def test_setup_namespace(client):
    #storage = AStorage(config)
    logger.info('creating namespace: %r' % namespace)
    client.setup_namespace(namespace, ['table1', 'table2'])
    logger.info('checking existence of tables')
    assert client.table_exists(ns('table1'))
    assert client.table_exists(ns('table2'))
    logger.info('finished checking')


def test_delete_namespace(client):
    storage = AStorage(config)
    storage.setup_namespace(namespace, ['table1', 'table2'])
    tables = client.list_tables()
    assert ns('table1') in tables
    assert ns('table2') in tables
    storage.delete_namespace(namespace)
    tables = client.list_tables()
    assert ns('table1') not in tables
    assert ns('table2') not in tables


def test_clear_table(client):
    storage = AStorage(config)
    storage.setup_namespace(namespace, ['table1'])

    # Write some rows to table
    m = Mutation('row_1')
    m.put(cf='cf1', cq='cq1', val='1')
    m.put(cf='cf1', cq='cq1', val='2')
    client.write(ns('table1'), m)

    # Clear table
    storage.clear_table('table1')

    # Verify clear
    for entry in client.scan(ns('table1')):
        assert False

    # Clear an empty table
    storage.clear_table('table1')

    # Verify still clear
    for entry in client.scan(ns('table1')):
        assert False


def test_create_if_missing(client):
    storage = AStorage(config)
    assert not client.table_exists(ns('table1'))
    storage.create_if_missing(namespace, 'table1', 2)
    assert client.table_exists(ns('table1'))
    storage.create_if_missing(namespace, 'table1', 2)
    assert client.table_exists(ns('table1'))


def test_put_get(client):
    storage = AStorage(config)
    storage.setup_namespace(namespace, ['table1'])
    kv_dict = {(uuid.uuid4(),
                uuid.uuid4()): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    storage.put('table1', *keys_and_values)
    keys = kv_dict.keys()
    values = kv_dict.values()
    for entry in client.scan(ns('table1')):
        assert entry.val in values
    generator = storage.get('table1', (keys[0], keys[0]))
    key, value = generator.next()
    assert kv_dict[key] == value
    for x in xrange(10):
        for key, value in storage.get('table1', (keys[x], keys[x])):
            assert kv_dict[key] == value


def test_delete(client):
    storage = AStorage(config)
    storage.setup_namespace(namespace, ['table1'])
    kv_dict = {(uuid.uuid4(),
                uuid.uuid4()): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    storage.put('table1', *keys_and_values)
    keys = kv_dict.keys()
    delete_keys = keys[0:len(keys):2]
    save_keys = keys[1:len(keys):2]
    assert set(delete_keys)
    storage.delete('table1', *delete_keys)
    for key in save_keys:
        for key, value in storage.get('table1', (key, key)):
            assert kv_dict[key] == value
    for key in delete_keys:
        generator = storage.get('table1', (key, key))
        with pytest.raises(StopIteration):
            generator.next()


def test_close(client):
    storage = AStorage(config)
    conn = storage.conn
    assert conn
    assert storage._connected
    storage.close()
    assert not storage._connected


def test_preceeding_key():
    storage = AStorage(config)
    assert storage._preceeding_key('00001') == '00000'
    assert storage._preceeding_key('00010') == '0000f'
    assert storage._preceeding_key('00011') == '00010'
    assert storage._preceeding_key('000f0') == '000ef'
    assert storage._preceeding_key('000f1') == '000f0'
    assert storage._preceeding_key('fff00') == 'ffeff'
    assert storage._preceeding_key('fff00') == 'ffeff'
    assert storage._preceeding_key('f0000') == 'effff'
