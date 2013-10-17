import pytest

from kvlayer._postgres import PGStorage, _valid_namespace

config_postgres = {
    'namespace': 'test',
    'storage_addresses': ['host=test-postgres.diffeo.com port=5432 user=test dbname=test password=test'],
}


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


@pytest.mark.parametrize(
    "namespace",
    ['_ok', 'Aok', 'aok'])
def test_legal_namespaces(namespace):
    assert _valid_namespace(namespace)
