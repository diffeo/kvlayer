from __future__ import absolute_import
import logging
import uuid
import kvlayer
from kvlayer._file_storage import FileStorage
import yakonfig

logger = logging.getLogger(__name__)

def test_persistence(tmpdir, namespace_string):
    config_yaml = """
kvlayer:
    storage_type: local
    storage_addresses: []
    namespace: {}
    app_name: kvlayer
    filename: {!s}
""".format(namespace_string, tmpdir.join('original'))
    with yakonfig.defaulted_config([kvlayer], yaml=config_yaml):
        ## Test that we can persist data to a file
        storage = FileStorage()
        storage.setup_namespace({'table1': 4, 'table2': 1})
        logger.info('checking existence of tables')

        storage.put('table1', *[((uuid.uuid4(), uuid.uuid4(),
                                  uuid.uuid4(), uuid.uuid4()), b'test_data')])
        results = list(storage.scan('table1'))
        assert len(results) == 1
        assert results[0][1] == 'test_data'

        ## Test that we can get same data to from file
        storage = FileStorage()
        storage.setup_namespace({'table1': 4, 'table2': 1})
        results2 = list(storage.scan('table1'))
        assert len(results2) == 1
        assert results2[0][1] == 'test_data'

    config_yaml = """
kvlayer:
    storage_type: local
    storage_addresses: []
    namespace: {}
    app_name: kvlayer
    filename: {!s}
    copy_to_filename: {!s}
""".format(namespace_string, tmpdir.join('original'), tmpdir.join('new'))
    with yakonfig.defaulted_config([kvlayer], yaml=config_yaml):
        ## Test that we can get can copy original tables to a new file
        storage = FileStorage()
        storage.setup_namespace({'table1': 4, 'table2': 1})
        results3 = list(storage.scan('table1'))
        assert len(results3) == 1
        assert results3[0][1] == 'test_data'

        ## Assert that all the results are the same
        assert results == results2 == results3
