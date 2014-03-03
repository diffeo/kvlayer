from __future__ import absolute_import
import uuid

import pytest

import kvlayer
from kvlayer._exceptions import MissingID
from kvlayer._local_memory import LocalStorage
import yakonfig

@pytest.yield_fixture(scope='module')
def local_storage():
    config_yaml = """
kvlayer:
    storage_type: local
    storage_addresses: []
    namespace: test
    app_name: test
"""
    with yakonfig.defaulted_config([kvlayer], yaml=config_yaml):
        local_storage = LocalStorage()
        yield local_storage

def test_local_storage_singleton(local_storage):
    local_storage.setup_namespace(dict(meta=1))
    keys_and_values = ((uuid.uuid4(),), b'hi')
    local_storage.put('meta', keys_and_values)
    key_range = (keys_and_values[0], keys_and_values[0])
    meta = list(local_storage.scan('meta', key_range))
    assert meta[0][1] == b'hi'
    local_storage2 = LocalStorage()
    local_storage2.setup_namespace(dict(meta=1))
    meta = list(local_storage2.scan('meta', key_range))
    assert meta[0][1] == b'hi'

def test_get(local_storage):
    local_storage.setup_namespace(dict(meta=1))
    value = b'hi2'
    keys = tuple([uuid.uuid4()])
    local_storage.put('meta', tuple((keys, value)))
    res = list(local_storage.get('meta', keys))
    for res_key, res_value in res:
        if res_key == keys:
            assert res_value == value
    assert keys in [rk for rk, rv in res]

def test_get_complex_keys(local_storage):
    local_storage.setup_namespace(dict(meta=2))
    value = b'hi2'
    key = (uuid.uuid4(), uuid.uuid4())
    local_storage.put('meta', tuple((key, value)))
    res = list(local_storage.get('meta', key))
    for res_key, res_value in res:
        if res_key == key:
            assert res_value == value
    assert key in [rk for rk, rv in res]

def test_delete_namespace(local_storage):
    """Test that delete_namespace() actually clears the shared storage"""
    u = (uuid.uuid4(),)

    local_storage.setup_namespace(dict(meta=1))
    local_storage.put('meta', (u, b'hi'))
    assert list(local_storage.get('meta', u)) == [(u, b'hi')]
    local_storage.delete_namespace()

    local_storage2 = LocalStorage()
    local_storage2.setup_namespace(dict(meta=1))
    with pytest.raises(MissingID):
        assert list(local_storage2.get('meta', u)) == []
