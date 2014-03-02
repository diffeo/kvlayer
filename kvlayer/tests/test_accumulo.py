import logging
import os
import re
import sys
import uuid

from pyaccumulo import Accumulo, Mutation
import pytest
import yaml

import kvlayer
from kvlayer._accumulo import AStorage, _string_decrement
from kvlayer._exceptions import MissingID
import kvlayer.tests.make_namespace

logger = logging.getLogger(__name__)

config_path = os.path.join(os.path.dirname(__file__), 'config_accumulo.yaml')
if not os.path.exists(config_path):
    sys.exit('failed to find %r' % config_path)

try:
    config = yaml.load(open(config_path))
except Exception, exc:
    sys.exit('failed to load %r: %s' % (config_path, exc))


def test_string_decrement():
    assert _string_decrement('b') == 'a'
    assert _string_decrement('b\0') == 'a\xff'
    assert _string_decrement('\0') == None
    assert _string_decrement('') == None
    assert _string_decrement('b\xff') == 'b\xfe'


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
            ## ...this *should* work via namespace_string fixture,
            ## but I'm not going to turn it on right now...?
            if re.search(namespace_string, table):
                conn.delete_table(table)

    ## see comment above about why this does not work
    #request.addfinalizer(fin)

    return conn


@pytest.fixture(scope='function', params=['accumulo'])
def client(namespace_string, request):

    global config
    config['app_name'] = 'kvlayer'
    config['namespace'] = namespace_string
    config['storage_type'] = request.param

    logger.info('initializing client')
    client = kvlayer.client(config)
    def _test_ns(name):
        return '_'.join([config['app_name'], config['namespace'], name])
    client._test_ns = _test_ns

    logger.info('deleting old namespace')
    #client.delete_namespace()

    def fin():
        logger.info('tearing down %s', _test_ns(''))
        client.delete_namespace()
        logger.info('done cleaning up')
    request.addfinalizer(fin)

    logger.info('starting test')
    return client


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
        logger.debug('found %r:%r', entry.row, entry.val)
        assert entry.val in values
    generator = client.scan('table1', (keys[0], keys[0]))
    key, value = generator.next()
    assert kv_dict[key] == value
    for x in xrange(10):
        for key, value in client.scan('table1', (keys[x], keys[x])):
            assert kv_dict[key] == value


def test_scan_all_keys(client, direct):
    client.setup_namespace({'table1': 1, 'table2': 1})
    kv_dict = {(uuid.uuid4(),): 'value' + str(x) for x in xrange(10)}
    keys_and_values = [(key, value) for key, value in kv_dict.iteritems()]
    client.put('table1', *keys_and_values)

    for key, val in client.scan('table1'):
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
        for key, value in client.scan('table1', (key, key)):
            assert kv_dict[key] == value
    for key in delete_keys:
        generator = client.scan('table1', (key, key))
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
