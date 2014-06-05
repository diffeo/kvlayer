import pytest
import logging

logger = logging.getLogger(__name__)

config_postgres = {
    'namespace': 'test',
    'storage_addresses': ['host=test-postgres.diffeo.com port=5432 user=test dbname=test password=test'],
}


try:
    from kvlayer._postgres import PGStorage, _valid_namespace, detatch_on_exception
    postgres_missing = 'False'
except ImportError:
    postgres_missing = 'True'

@pytest.mark.skipif(postgres_missing)
@pytest.mark.parametrize(
    "badnamespace",
    [None,
     '9isnotaletter',
     '$isnotaletter',
 ])
def test_illegal_namespaces(badnamespace):
    with pytest.raises(Exception):
        config = dict(config_postgres)
        config['namespace'] = badnamespace
        pg = PGStorage(config)


@pytest.mark.skipif(postgres_missing)
@pytest.mark.parametrize(
    "namespace",
    ['_ok', 'Aok', 'aok'])
def test_legal_namespaces(namespace):
    assert _valid_namespace(namespace)



class Closeable(object):
    def __init__(self):
        self.closed = False

    @detatch_on_exception
    def err(self):
        raise Exception('err')

    def close(self):
        self.closed = True


@pytest.mark.skipif(postgres_missing)
def test_detatch_on_exception():
    a = Closeable()
    with pytest.raises(Exception):
        a.err()
    assert a.closed


class NotCloseable(object):
    @detatch_on_exception
    def errX(self):
        raise Exception('err')

@pytest.mark.skipif(postgres_missing)
def test_detatch_on_exception2():
    # check that @detatch_on_exception doesn't panic when there's no self.close
    a = NotCloseable()
    with pytest.raises(Exception):
        a.errX()
