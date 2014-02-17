'''
Implementation of AbstractStorage using python shelve module

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
import shutil
import shelve
import logging
import cPickle
from kvlayer._local_memory import LocalStorage
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection

logger = logging.getLogger('kvlayer')

class FileStorage(LocalStorage):
    '''
    File storage for testing and development
    '''
    def __init__(self, config):
        ## singleton prevents use of super
        #super(FileStorage, self).__init__(config)
        AbstractStorage.__init__(self, config)

        filename = config['filename']
        if config.get('copy_to_filename', False):
            copy_to_filename  = config['copy_to_filename']
            shutil.copyfile(filename, copy_to_filename)
            filename = copy_to_filename

        if os.path.exists(filename) and os.path.getsize(filename) == 0:
            os.remove(filename)
        logger.debug('Opening %s for kvlayer file storage', filename)
        self._data = shelve.open(filename,
                                protocol= cPickle.HIGHEST_PROTOCOL,
                                writeback=True)
        self._table_names = {}

    def setup_namespace(self, table_names):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        logger.debug('creating tables: %r', table_names)
        self._table_names.update(table_names)
        ## just store everything in a dict
        for table in table_names:
            if table not in self._data:
                self._data[table] = dict()
        self._connected = True

    def delete_namespace(self):
        self._data.clear()

    def delete(self, table_name, *keys):
        for key in keys:
            self._data[table_name].pop(key, None)
