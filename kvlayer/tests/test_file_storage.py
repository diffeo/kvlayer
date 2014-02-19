from __future__ import absolute_import
import logging
import uuid
from kvlayer._file_storage import FileStorage

logger = logging.getLogger(__name__)

def test_persistence(tmpdir, namespace_string):
    ## Test that we can persist data to a file
    config = dict(
        filename = str(tmpdir.join('original')),
        namespace = namespace_string,
        app_name = 'kvlayer'
    )

    storage = FileStorage(config)
    storage.setup_namespace({'table1': 4, 'table2': 1})
    logger.info('checking existence of tables')

    storage.put('table1', *[((uuid.uuid4(), uuid.uuid4(),
                        uuid.uuid4(), uuid.uuid4()), b'test_data')])
    results = list(storage.scan('table1'))
    assert len(results) == 1
    assert results[0][1] == 'test_data'


    ## Test that we can get same data to from file
    storage = FileStorage(config)
    storage.setup_namespace({'table1': 4, 'table2': 1})
    results2 = list(storage.scan('table1'))
    assert len(results2) == 1
    assert results2[0][1] == 'test_data'

    ## Test that we can get can copy original tables to a new file
    config = dict(
        filename = str(tmpdir.join('original')),
        copy_to_filename = str(tmpdir.join('new')),
        namespace = namespace_string,
        app_name = 'kvlayer'
    )

    storage = FileStorage(config)
    storage.setup_namespace({'table1': 4, 'table2': 1})
    results3 = list(storage.scan('table1'))
    assert len(results3) == 1
    assert results3[0][1] == 'test_data'

    ## Assert that all the results are the same
    assert results == results2 == results3
