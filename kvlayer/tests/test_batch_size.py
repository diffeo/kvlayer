'''performance tests around batch size

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import division, absolute_import
import argparse
import cProfile
import logging
import sys
import time

import pytest

import kvlayer
from kvlayer.tests.test_interface import client, backend  # fixture
import yakonfig

logger = logging.getLogger(__name__)


def scan_batch_size(client):
    client.setup_namespace(dict(t1=(int,)))

    one_mb = r' ' * 2**20
    total_inserts = 200
    tasks = [((x,), one_mb) for x in xrange(total_inserts)]

    for batch_size in range(1, 100):        
        start_time = time.time()
        for start in range(0, total_inserts, batch_size):
            client.put('t1', *tasks[start:start+batch_size])
        elapsed = time.time() - start_time
        rate = total_inserts / elapsed
        print('%d MB written in %.1f seconds --> '
              '%.1f MB/sec with batch_size=%d for storage_type=%s' % (
                  total_inserts, elapsed, rate, batch_size,
                  client._config['storage_type']))

    #assert len(ret_vals) == total_inserts

    '''
        start_time = time.time()
        count = 0
        for (found, u) in run_many(many_gets, ret_vals, 
                                   class_config=client._config,
                                   num_workers=num_workers,
                                   timeout=total_inserts * 5,
                                   profile=profile):
            if not found:
                raise Exception('failed to find %r' % u)
            count += 1
        elapsed = time.time() - start_time
        assert len(ret_vals) == total_inserts
        rate = total_inserts / elapsed
        logger.info('%d MB read in %.1f seconds --> '
                    '%.1f MB/sec across %d workers for storage_type=%s',
                    total_inserts, elapsed, rate, num_workers,
                    client._config['storage_type'])

    '''

def main():
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--storage_type', default='redis')
    parser.add_argument('--storage_address', nargs='?', dest='storage_addresses')
    modules = [yakonfig, kvlayer]
    args = yakonfig.parse_args(parser, modules)
    config = yakonfig.get_global_config()

    if not args.storage_addresses:
        args.storage_addresses = ['redis.diffeo.com:6379']

    config['kvlayer'].update({
        'storage_type': args.storage_type,
        'storage_addresses': args.storage_addresses,
        })
    client = kvlayer.client()
    scan_batch_size(client)

if __name__ == '__main__':
    main()
