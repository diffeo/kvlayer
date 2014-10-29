#!/usr/bin/env python
'''Performance tests for kvlayer.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

This contains various performance-oriented tests that print statistics
to standard output.  If run as a standalone program, it takes no
configuration options, but it does look for ``config_*.yaml`` files in
the test directory and runs tests against every backend that it can
find configuration for.  Returns 1 if any backend encountered an
exception, or 0 otherwise.

'''
from __future__ import absolute_import, division, print_function
import argparse
import cProfile
import multiprocessing
import os
import Queue
import sys
import time
import traceback
import uuid

try:
    import dblogger
except ImportError:
    dblogger = None
import yakonfig

import kvlayer
from kvlayer._client import STORAGE_CLIENTS


# Straightforward performance-oriented tests

def perftest_large_writes(client):
    client.setup_namespace(dict(t1=2, t2=3))

    u1, u2 = uuid.uuid1(), uuid.uuid1()

    fifteen_mb_string = b' ' * 15 * 2 ** 20
    # chop off the end to leave room for thrift message overhead
    long_string = fifteen_mb_string[: -256]

    num = 10
    total_bytes = num * len(long_string)
    print('Testing large writes ({} rows of {} KB)'
          .format(num, len(long_string)/1024))

    t1 = time.time()

    for rows in xrange(num):
        client.put('t1', ((u1, u2), long_string))
    t2 = time.time()

    dt = t2 - t1
    print ('{} single  rows put={:.1f} sec ({:.1f} per sec, {:.1f} KB/s)'
           .format(num, dt, num / dt, total_bytes / (1024 * dt)))

    client.put('t1', *(((u1, u2), long_string) for row in xrange(num)))
    t3 = time.time()

    dt = t3 - t2
    print ('{} batched rows put={:.1f} sec ({:.1f} per sec, {:.1f} KB/s)'
           .format(num, dt, num / dt, total_bytes / (1024 * dt)))


def perftest_storage_speed(client):
    client.setup_namespace(dict(t1=2, t2=3))
    num_rows = 10 ** 4
    print('Testing storage I/O ({} rows)'.format(num_rows))
    t1 = time.time()
    client.put('t1', *[((uuid.uuid4(), uuid.uuid4()), b'')
                       for i in xrange(num_rows)])
    t2 = time.time()
    results = list(client.scan('t1', batch_size=num_rows))
    t3 = time.time()
    assert num_rows == len(results)
    put_rate = float(num_rows) / (t2 - t1)
    get_rate = float(num_rows) / (t3 - t2)
    print('{} rows '
          'put={:.1f} sec ({:.1f} per sec) '
          'get={:.1f} sec ({:.1f} per sec)'
          .format(num_rows, (t2 - t1), put_rate, (t3 - t2), get_rate))


# The giant complex multiprocessing-based concurrent performance test

def run_many(operation, task_generator, timeout, num_workers=1,
             class_config=None, profile=False):
    '''Run some operation function in parallel under multiprocessing.

    Runs `num_workers` child processes and sends them tasks from
    `task_generator`.  This is a generator function that yields the
    results.

        for response from run_many(do_search, queries, num_workers=10,
                                   timeout=5):
            validate(response)

    `operation` is a callable of two parameters, a task from
    `task_generator` and the output queue `o_queue`.  Alternately,
    `operation` can be a class object (or other callable that returns
    a callable of this form), and `class_config` will be passed to it.

    :param operation: callable to do work in the worker
    :param task_generator: iterable of input parameters to `operation`
    :param int timeout: maximum run time, in seconds
    :param int num_workers: number of child processes to run in parallel
    :param class_config: if not :const:`None`, expect `operation` to
      be a class, and pass this to its constructor
    :param bool profile: if true, run workers under a profiler
      and write to pid-specific files in the current directory
    :return: 0 on success, 1 on child process exception, 2 on timeout

    '''

    task_generator = iter(task_generator)

    manager = multiprocessing.Manager()
    tasks_remaining = manager.Event()
    tasks_remaining.set()

    # make I/O queues with no size constraint
    i_queue = manager.Queue()  # pylint: disable=E1101
    o_queue = manager.Queue()  # pylint: disable=E1101

    pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)

    # load up the pool
    async_results = []
    for x in range(num_workers):
        async_res = pool.apply_async(
            worker,
            args=(operation, i_queue, o_queue, tasks_remaining, class_config,
                  profile))
        async_results.append(async_res)

    start_time = time.time()
    while async_results:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            break
        for async_res in async_results:
            try:
                async_res.get(0)
            except multiprocessing.TimeoutError:
                pass
            except Exception:
                print('*** multiprocessing worker failed')
                async_results.remove(async_res)
            else:
                assert async_res.ready()
                async_results.remove(async_res)
        while elapsed < timeout:
            elapsed = time.time() - start_time
            try:
                resp = o_queue.get(block=False)
                yield resp
            except Queue.Empty:
                time.sleep(0.2)
                break
        tasks_added = 0
        while (o_queue.qsize() < num_workers * 2 and elapsed < timeout
               and tasks_added < num_workers * 2):
            elapsed = time.time() - start_time
            task = next(task_generator, None)
            if task is None:
                tasks_remaining.clear()
                break
            else:
                i_queue.put(task)
                tasks_added += 1

    pool.close()
    if async_results:
        # Tests timed out, things are still running
        pool.terminate()
    pool.join()


def worker(operation, i_queue, o_queue, tasks_remaining,
           class_config=None, profile=False):
    '''Simple task runner.

    This is intended to run as a single thread under :mod:`multiprocessing`.
    `operation` is a callable of two parameters, a task pulled from
    `i_queue` and the output queue `o_queue`.  Alternately, `operation`
    can be a class object (or other callable that returns a callable of
    this form), and `class_config` will be passed to it.

    :param operation: callable to do work in the worker
    :param i_queue: queue of input tasks
    :type i_queue: :class:`multiprocessing.Queue`
    :param o_queue: queue of results
    :type o_queue: :class:`multiprocessing.Queue`
    :param tasks_remaining: if not set, wait for more work, even on an
      empty queue
    :type tasks_remaining: :class:`multiprocessing.Event`
    :param class_config: if not :const:`None`, expect `operation` to
      be a class, and pass this to its constructor
    :param bool profile: if true, run this worker under a profiler
      and write to a pid-specific file in the current directory

    '''
    pr = None
    if profile:
        pr = cProfile.Profile()
        pr.enable()
    if class_config is not None:
        # operation is a class with __call__ so initialize it
        operation = operation(class_config)
    while 1:
        try:
            task = i_queue.get(timeout=0.3)
            operation(task, o_queue)
        except Queue.Empty:
            if tasks_remaining.is_set():
                continue
            else:
                break
    if pr:
        pr.create_stats()
        pr.dump_stats('profile.%d.txt' % os.getpid())


class random_inserts(object):
    '''Worker function that inserts 1 MB keys in a table.'''
    def __init__(self, config):
        self.client = kvlayer.client()
        self.client.setup_namespace(dict(t1=1))
        self.one_mb = ' ' * 2**20

    def __call__(self, u, o_queue):
        self.client.put('t1', ((u,), self.one_mb))
        o_queue.put(u)


class many_gets(object):
    '''Worker function that retrieves keys from a table.'''
    def __init__(self, config):
        self.client = kvlayer.client()
        self.client.setup_namespace(dict(t1=1))

    def __call__(self, u, o_queue):
        list(self.client.get('t1', (u,)))
        o_queue.put((True, u))


def perftest_throughput_insert_random(num_workers=4,
                                      num_inserts=100, profile=False):
    '''Measure concurrent write throughput writing data to a table.'''
    total_inserts = num_workers * num_inserts
    task_generator = (uuid.uuid4() for x in xrange(total_inserts))
    start_time = time.time()
    ret_vals = list(run_many(random_inserts, task_generator,
                             timeout=total_inserts * 5,
                             class_config={},
                             num_workers=num_workers,
                             profile=profile))
    elapsed = time.time() - start_time
    assert len(ret_vals) == total_inserts
    rate = total_inserts / elapsed
    print('{} MB written in {:.1f} seconds --> '
          '{:.1f} MB/sec across {} workers'
          .format(total_inserts, elapsed, rate, num_workers))
    return ret_vals


def perftest_throughput_many_gets(ret_vals=[], num_workers=4,
                                  profile=False):
    '''Measure concurrent read throughput reading data from a table.'''
    start_time = time.time()
    count = 0
    for (found, u) in run_many(many_gets, ret_vals,
                               class_config={},
                               num_workers=num_workers,
                               timeout=len(ret_vals) * 5,
                               profile=profile):
        assert found
        count += 1
    elapsed = time.time() - start_time
    assert count == len(ret_vals)
    rate = count / elapsed
    print('{} MB read in {:.1f} seconds --> '
          '{:.1f} MB/sec across {} workers'
          .format(count, elapsed, rate, num_workers))


def run_perftests(num_workers=4, num_inserts=100, profile=False):
    '''Run all of the performance tests.

    Must be run in a :mod:`yakonfig` context.

    :return: 0 on success, 1 on any exception

    '''
    rc = 0
    name = yakonfig.get_global_config('kvlayer')['storage_type']
    print('Running tests on backend "{}"'.format(name))
    try:
        client = kvlayer.client()
        client.delete_namespace()
        perftest_large_writes(client)
        client.delete_namespace()
        perftest_storage_speed(client)
        client.delete_namespace()
        if name not in ['filestorage']:
            ret_vals = perftest_throughput_insert_random()
            perftest_throughput_many_gets(ret_vals)
        client.close()
    except Exception:
        traceback.print_exc()
        rc = 1
    print('')
    return rc


def run_all_perftests(redis_address=None):
    '''Run all of the performance tests, on every backend.

    This is intended to be a fully-automated answer for
    :program:`kvlayer_test`.

    '''
    rc = 0
    for name in STORAGE_CLIENTS.iterkeys():
        if name in ['cassandra', 'accumulo', 'postgres']:
            continue
        config = os.path.join(os.path.dirname(__file__),
                              'config_{}.yaml'.format(name))
        if not os.path.exists(config):
            continue
        params = {'app_name': 'kvlayer_performance',
                  'namespace': 'ns' + uuid.uuid1().hex}
        if name == 'filestorage':
            params['kvlayer_filename'] = os.tmpnam()
        if name == 'redis' and redis_address is not None:
            params['storage_addresses'] = [redis_address]
        with yakonfig.defaulted_config(
                [kvlayer], filename=config, params=params):
            ret = run_perftests()
            if rc == 0:
                rc = ret
        if name == 'filestorage':
            os.unlink(params['kvlayer_filename'])
    return rc


def main():
    parser = argparse.ArgumentParser(
        description='Run kvlayer performance tests on a single backend.',
        conflict_handler='resolve')
    parser.add_argument('--num-workers', default=4, type=int)
    parser.add_argument('--num-inserts', default=100, type=int)
    parser.add_argument('--profile', action='store_true')
    modules = [yakonfig]
    if dblogger:
        modules.append(dblogger)
    modules.append(kvlayer)
    args = yakonfig.parse_args(parser, modules)

    return run_perftests(num_workers=args.num_workers,
                         num_inserts=args.num_inserts,
                         profile=args.profile)


if __name__ == '__main__':
    sys.exit(main())
