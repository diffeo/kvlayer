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
    '''
    def __init__(self, *args, **kwargs):
        super(FileStorage, self).__init__(*args, **kwargs)

        filename = self._config['filename']
        if self._config.get('copy_to_filename', False):
            copy_to_filename  = self._config['copy_to_filename']
            shutil.copyfile(filename, copy_to_filename)
            filename = copy_to_filename

        if os.path.exists(filename) and os.path.getsize(filename) == 0:
            os.remove(filename)
        # logger.debug('Opening %s for kvlayer file storage', filename)
        self._data = shelve.open(filename,
                                protocol= cPickle.HIGHEST_PROTOCOL,
                                writeback=True)
        self._table_names = {}

    def delete_namespace(self):
        self._data.clear()
