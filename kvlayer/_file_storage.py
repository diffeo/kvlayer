'''
Implementation of AbstractStorage using python shelve module

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import os
import shutil
import shelve
import logging
import cPickle
from kvlayer._local_memory import AbstractLocalStorage

logger = logging.getLogger(__name__)


class FileStorage(AbstractLocalStorage):
    '''
    File storage for testing and development

    Note that there should only be one connection to any particular
    file-backed storage at a time. (Data lives in memory and is synced
    to disk in a way that will clobber whatever is already there.)
    '''

    _datas = {}

    def __init__(self, *args, **kwargs):
        super(FileStorage, self).__init__(*args, **kwargs)

        filename = self._config['filename']
        if filename in self._datas:
            self._data = self._datas[filename]
        else:
            if self._config.get('copy_to_filename', False):
                copy_to_filename = self._config['copy_to_filename']
                shutil.copyfile(filename, copy_to_filename)
                filename = copy_to_filename

            if os.path.exists(filename) and os.path.getsize(filename) == 0:
                os.remove(filename)
            self._data = shelve.open(filename,
                                     protocol=cPickle.HIGHEST_PROTOCOL,
                                     writeback=True)
            self._datas[filename] = self._data

        self._table_names = {}

    def delete_namespace(self):
        self._data.clear()

    def put(self, table_name, *keys_and_values, **kwargs):
        super(FileStorage, self).put(table_name, *keys_and_values, **kwargs)
        self._data.sync()

    def delete(self, table_name, *keys):
        super(FileStorage, self).delete(table_name, *keys)
        self._data.sync()

    def clear_table(self, table_name):
        super(FileStorage, self).clear_table(table_name)
        self._data.sync()
