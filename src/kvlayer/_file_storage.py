'''
Implementation of AbstractStorage using python shelve module

Your use of this software is governed by your license agreement.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
import uuid
import shutil
import shelve
import logging
import cPickle
from kvlayer._exceptions import MissingID
from kvlayer._abstract_storage import AbstractStorage
from kvlayer._utils import _requires_connection

logger = logging.getLogger('kvlayer')

## make FileStorage a singleton, so it looks like a client to db
def _file_storage_singleton(cls):
    instances = {}
    def getinstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        ## make FileStorage look like other storage clients by having
        ## it not be connected until you re-access it:
        instances[cls]._connected = True
        return instances[cls]
    return getinstance

@_file_storage_singleton
class FileStorage(AbstractStorage):
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

    @_requires_connection
    def delete_namespace(self):
        self._data.clear()

    @_requires_connection
    def clear_table(self, table_name):
        self._data[table_name] = dict()

    @_requires_connection
    def put(self, table_name, *keys_and_values, **kwargs):
        count = 0
        for key, val in keys_and_values:
            ex = self.check_put_key_value(key, val, table_name, self._table_names[table_name])
            if ex:
                raise ex
            self._data[table_name][key] = val
            count += 1

    @_requires_connection
    def get(self, table_name, *key_ranges, **kwargs):
        key_ranges = list(key_ranges)
        if not key_ranges:
            key_ranges = [[('',), ('',)]]
        for start, finish in key_ranges:
            found_in_range = []
            for key, val in self._data[table_name].iteritems():
                if start == ('',) == finish:
                    ## this is the base case
                    yield key, val
                    continue
                ## given a range, mimic the behavior of DBs that tell
                ## you if they failed to find a key
                within_range = True
                for i in range(len(start)):
                    if not (start[i] <= key[i] <= finish[i]):
                        within_range = False
                        break
                if within_range:
                    found_in_range.append( (key, val) )
            if found_in_range:
                for key, val in found_in_range:
                    yield key, val
            elif start != ('',):
                ## specified a key range, but found none
                raise MissingID()

    @_requires_connection
    def delete(self, table_name, *keys):
        for key in keys:
            self._data[table_name].pop(key, None)

    def close(self):
        ## prevent reading until connected again
        self._connected = False
