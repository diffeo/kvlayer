from __future__ import absolute_import
import uuid
from kvlayer._file_storage import FileStorage

def test_persistence(tmpdir, namespace_string):
    filename = str(tmpdir.join('original'))

    ## Test that we can persist data to a file
    storage = FileStorage(config={'filename': filename},
                          app_name='kvlayer', namespace=namespace_string)
    storage.setup_namespace({'table1': 4, 'table2': 1})

    storage.put('table1', *[((uuid.uuid4(), uuid.uuid4(),
                              uuid.uuid4(), uuid.uuid4()), b'test_data')])
    results = list(storage.scan('table1'))
    assert len(results) == 1
    assert results[0][1] == 'test_data'

    ## Test that we can get same data to from file
    storage = FileStorage(config={'filename': filename},
                          app_name='kvlayer', namespace=namespace_string)
    storage.setup_namespace({'table1': 4, 'table2': 1})
    results2 = list(storage.scan('table1'))
    assert len(results2) == 1
    assert results2[0][1] == 'test_data'

    ## Test that we can get can copy original tables to a new file
    copy_filename = str(tmpdir.join('new'))
    storage = FileStorage(config={'filename': filename,
                                  'copy_to_filename': copy_filename},
                          app_name='kvlayer', namespace=namespace_string)
    storage.setup_namespace({'table1': 4, 'table2': 1})
    results3 = list(storage.scan('table1'))
    assert len(results3) == 1
    assert results3[0][1] == 'test_data'

    ## Assert that all the results are the same
    assert results == results2 == results3
