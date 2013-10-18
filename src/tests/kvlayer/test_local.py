import os
import sys
import time
import yaml
import uuid
import pytest
import kvlayer
from kvlayer import MissingID
from kvlayer._local_memory import LocalStorage
from make_namespace_string import make_namespace_string

from _setup_logging import logger

config_local = dict(
    storage_type='local',
    namespace='test',
    app_name='test',
    )

def test_local_storage_singleton():
    local_storage = LocalStorage(config_local)
    local_storage.setup_namespace(dict(meta=1))
    keys_and_values = ((uuid.uuid4(),), b'hi')
    local_storage.put('meta', keys_and_values)
    key_range = (keys_and_values[0], keys_and_values[0])
    meta = list(local_storage.get('meta', key_range))
    assert meta[0][1] == b'hi'
    local_storage2 = LocalStorage(config_local)
    meta = list(local_storage2.get('meta', key_range))
    assert meta[0][1] == b'hi'

