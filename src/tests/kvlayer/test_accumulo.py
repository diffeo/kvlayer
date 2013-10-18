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
from kvlayer._exceptions import MissingID
from make_namespace_string import make_namespace_string

config_path = os.path.join(os.path.dirname(__file__), 'config_accumulo.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))


@pytest.fixture
def direct(request):
    conn = Accumulo(host='test-accumulo-1.diffeo.com', port=50096,
                    user=config['username'], password=config['password'])

    def fin():
        conn = Accumulo(host=config['host'], port=50096,
                        user=config['username'], password=config['password'])
        tables = conn.list_tables()
        for table in tables:
            ## would need to get namespace variable here for this
            ## finalizer to work...
            if re.search(namespace, table):
                conn.delete_table(table)

    ## see comment above about why this does not work
    #request.addfinalizer(fin)

    return conn


@pytest.fixture(scope='function', params=['accumulo'])
def client(request):

    global config
    config['namespace'] = make_namespace_string()
    config['storage_type'] = request.param

    logger.info('initializing client')
    client = kvlayer.client(config)
    client._test_ns = lambda name: name + '_' + config['namespace']

    logger.info('deleting old namespace')
    #client.delete_namespace()

    def fin():
        logger.info('tearing down %r' % config['namespace'])
        client.delete_namespace()
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


def test_setup_namespace(client, direct):
    #storage = AStorage(config)
    logger.info('creating namespace: %r' % client._namespace)
    client.setup_namespace({'table1': 1, 'table2': 1})
    logger.info('checking existence of tables')
    assert direct.table_exists(client._test_ns('table1'))
    assert direct.table_exists(client._test_ns('table2'))
    logger.info('finished checking')


def test_delete_namespace(client, direct):
    storage = AStorage(config)
    client.setup_namespace({'table1': 1, 'table2': 1})
    tables = direct.list_tables()
    assert client._test_ns('table1') in tables
    assert client._test_ns('table2') in tables
    storage.delete_namespace()
    tables = direct.list_tables()
    assert client._test_ns('table1') not in tables
    assert client._test_ns('table2') not in tables


def test_clear_table(client, direct):
    client.setup_namespace({'table1': 1, 'table2': 1})

    # Write some rows to table
    m = Mutation('row_1')
    m.put(cf='cf1', cq='cq1', val='1')
    m.put(cf='cf1', cq='cq1', val='2')
    direct.write(client._test_ns('table1'), m)

    # Clear table
    client.clear_table('table1')

    # Verify clear
    for entry in direct.scan(client._test_ns('table1')):
        assert False

    # Clear an empty table
    client.clear_table('table1')

    # Verify still clear
    for entry in direct.scan(client._test_ns('table1')):
        assert False


def test_put_get(client, direct):
    client.setup_namespace({'table1': 1, 'table2': 1})
    kv_dict = {(uuid.uuid4(),): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    client.put('table1', *keys_and_values)
    keys = kv_dict.keys()
    values = kv_dict.values()
    for entry in direct.scan(client._test_ns('table1')):
        assert entry.val in values
    generator = client.get('table1', (keys[0], keys[0]))
    key, value = generator.next()
    assert kv_dict[key] == value
    for x in xrange(10):
        for key, value in client.get('table1', (keys[x], keys[x])):
            assert kv_dict[key] == value


def test_get_all_keys(client, direct):
    client.setup_namespace({'table1': 1, 'table2': 1})
    kv_dict = {(uuid.uuid4(),): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    client.put('table1', *keys_and_values)

    for key, val in client.get('table1'):
        assert kv_dict[key] == val
        del kv_dict[key]

    assert kv_dict == {}


def test_delete(client, direct):
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
        for key, value in client.get('table1', (key, key)):
            assert kv_dict[key] == value
    for key in delete_keys:
        generator = client.get('table1', (key, key))
        with pytest.raises(MissingID):
            row = generator.next()
            assert not row


def test_close(client):
    conn = client.conn
    assert conn
    assert client._connected
    client.close()
    assert not client._connected


def test_preceeding_key(client):
    assert client._preceeding_key('00001') == '00000'
    assert client._preceeding_key('00010') == '0000f'
    assert client._preceeding_key('00011') == '00010'
    assert client._preceeding_key('000f0') == '000ef'
    assert client._preceeding_key('000f1') == '000f0'
    assert client._preceeding_key('fff00') == 'ffeff'
    assert client._preceeding_key('fff00') == 'ffeff'
    assert client._preceeding_key('f0000') == 'effff'
    assert client._preceeding_key('00000') == '.'
