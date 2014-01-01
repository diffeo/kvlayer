from __future__ import division, absolute_import
import uuid
import time
import Queue
import pytest
import kvlayer
import multiprocessing
from signal import alarm, signal, SIGHUP, SIGTERM, SIGABRT, SIGALRM
from kvlayer import MissingID
from tests.kvlayer.test_interface import client # fixture
from tests.kvlayer._setup_logging import logger


def worker(operation, i_queue, o_queue, tasks_remaining,
           class_config=None,
    ):
    '''simple worker used by run_many below

@operation(task, o_queue): callable that takes "task" received from
i_queue and optionally passes something to o_queue

@i_queue: a multiprocessing Queue provided by run_many
@o_queue: a multiprocessing Queue provided by run_many
@tasks_remaining: a multiprocessing Event provided by run_many

@class_config: if not None, then operation is initialized by calling
operation(class_config) before using it inside each worker.

    '''
    if class_config is not None:
        ## operation is a class with __call__ so initialize it
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

def run_many(operation, task_generator, num_workers=1, timeout=None,
             class_config=None,
    ):
    '''test harness that runs num_workers as child processes and sends
them tasks from task_generator via queue.  Results are harvested from
o_queue and yielded by this function, so typical usage is:

for response from run_many(do_search, queries, num_workers=10, timeout=5):
    validate(response)

@operation: defined above

@task_generator: an iterable of things to pass through i_queue to workers

@num_workers: number of child processes to run in parallel, default is 1

@timeout: (required) tests must set an explicit timeout in seconds

@class_config: defined above
    '''
    if timeout is None:
        raise Exception('tests must set an explicit timeout')

    task_generator = iter(task_generator)

    manager = multiprocessing.Manager()
    tasks_remaining = manager.Event()
    tasks_remaining.set()

    ## make I/O queues with no size constraint
    i_queue = manager.Queue() # pylint: disable=E1101
    o_queue = manager.Queue() # pylint: disable=E1101

    pool = multiprocessing.Pool(num_workers, maxtasksperchild=1)

    ## prepare to catch control-C
    global run_many_not_killed
    run_many_not_killed = True
    def clean_exit(sig_num, frame):
        global run_many_not_killed
        run_many_not_killed = False
        logger.critical('attempting clean_exit')
        pool.terminate()
        logger.critical('pool terminated')
        pool.join()
        logger.critical('pool joined. Exiting.')
    for sig_num in [SIGTERM, SIGHUP, SIGABRT, SIGALRM]:
        signal(sig_num, clean_exit)

    ## load up the pool
    async_results = []
    for x in range(num_workers):
        async_res = pool.apply_async(
            worker, 
            args=(operation, i_queue, o_queue, tasks_remaining, class_config))
        async_results.append(async_res)

    start_time = time.time()
    while run_many_not_killed and async_results:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise Exception('%.1f seconds elapsed' % elapsed)
        for async_res in async_results:
            try:
                async_res.get(0)
            except multiprocessing.TimeoutError:
                logger.info('worker in progress')
            except Exception, exc:
                logger.info('worker died!', exc_info=True)
                raise
            else:
                logger.info('worker finished')
                assert async_res.ready()
                async_results.remove(async_res)
        while elapsed < timeout:
            elapsed = time.time() - start_time
            try:
                resp = o_queue.get(block=False)
                yield resp
            except Queue.Empty:
                logger.info('no responses from o_queue')
                time.sleep(0.2)
                break
        tasks_added = 0
        while o_queue.qsize() < num_workers * 2 and elapsed < timeout \
              and tasks_added < num_workers * 2:
            elapsed = time.time() - start_time
            try:
                task = task_generator.next()
                i_queue.put(task)
                tasks_added += 1
            except StopIteration:
                tasks_remaining.clear()
                break

    pool.close()
    pool.join()
    logger.info('finished running %d worker processes' % num_workers)


def pass_through(task, o_queue):
    o_queue.put(task)

def sleeper(task, o_queue):
    time.sleep(4)

def test_multiprocessing_harness():
    num_workers = 10
    task_generator = range(100)
    ret_vals = list(run_many(pass_through, task_generator, 
                             num_workers=num_workers, timeout=20))
    assert set(ret_vals) == set(range(100)), set(range(100)) - set(ret_vals)

def test_multiprocessing_harness_timeout():
    num_workers = 10
    task_generator = range(100)
    with pytest.raises(Exception) as exc:
        list(run_many(sleeper, task_generator, 
                      num_workers=num_workers, timeout=2))
    assert 'seconds elapsed' in str(exc)

def test_multiprocessing_harness_control_C():
    num_workers = 10
    task_generator = range(100)
    alarm(1)
    ret_vals = list(run_many(sleeper, task_generator, 
                             num_workers=num_workers, timeout=20))
    assert len(ret_vals) == 0


class random_inserts(object):
    def __init__(self, config):
        self.client = kvlayer.client(config)
        self.client.setup_namespace(dict(t1=1))
        self.one_mb = ' ' * 2**20

    def __call__(self, u, o_queue):
        try:
            self.client.put('t1', ((u,), self.one_mb))
        except Exception, exc:
            logger.critical('client failed!', exc_info=True)
            raise exc
        o_queue.put(u)
        logger.info('put one_mb at %r', u)


class many_gets(object):
    def __init__(self, config):
        self.client = kvlayer.client(config)
        self.client.setup_namespace(dict(t1=1))

    def __call__(self, u, o_queue):
        try:
            kvs = list(self.client.get('t1', (u,)))
        except Exception, exc:
            logger.critical('client failed!', exc_info=True)
            raise exc
        assert len(kvs[0][1]) == 2**20
        o_queue.put((True, u))
        logger.info('retrievied one_mb at %r', u)


def test_throughput_insert_random(client):
    client.setup_namespace(dict(t1=1))
    
    num_workers = 5
    num_inserts = 100
    total_inserts = num_workers * num_inserts
    task_generator = [uuid.uuid4() for x in xrange(total_inserts)]
    start_time = time.time()
    ret_vals = list(run_many(random_inserts, task_generator, 
                             class_config=client._config,
                             num_workers=num_workers, timeout=total_inserts/2))
    elapsed = time.time() - start_time
    assert len(ret_vals) == total_inserts
    rate = total_inserts / elapsed
    logger.info('%d MB written in %.1f seconds --> %.1f MB/sec across %d workers for storage_type=%s',
                total_inserts, elapsed, rate, num_workers, client._config['storage_type'])

    if client._config['storage_type'] in ['postgres', 'accumulo', 'cassandra']:
        start_time = time.time()
        count = 0
        for (found, u) in run_many(many_gets, ret_vals, 
                                   class_config=client._config,
                                   num_workers=num_workers, timeout=num_inserts):
            if not found:
                raise Exception('failed to find %r' % u)
            count += 1
        elapsed = time.time() - start_time
        assert len(ret_vals) == total_inserts
        rate = total_inserts / elapsed
        logger.info('%d MB written in %.1f seconds --> %.1f MB/sec across %d workers for storage_type=%s',
                    total_inserts, elapsed, rate, num_workers, client._config['storage_type'])


class indexer(object):
    '''
    rapidly changes an index in t2
    '''
    def __init__(self, config):
        self.client = kvlayer.client(config)
        self.client.setup_namespace(dict(t1=1, t2=2))

    def __call__(self, u, o_queue):
        try:
            kvs = list(self.client.scan('t2', ((u,), (u,))))
        except Exception, exc:
            logger.critical('client failed!', exc_info=True)
            raise exc
        u1, u2 = kvs[0][0]
        assert u == u1

        #... need to populate t2 initially, and then flow records through it from 0 to 1-8, to 9-24, 25-57, 58-121, etc.


class joiner(object):
    def __init__(self, config):
        self.client = kvlayer.client(config)
        self.client.setup_namespace(dict(t1=1, t2=2))

    def __call__(self, u, o_queue):
        try:
            kvs = list(self.client.scan('t2', ((u,), (u,))))
        except Exception, exc:
            logger.critical('client failed!', exc_info=True)
            raise exc
        u1, u2 = kvs[0][0]
        assert u == u1
        try:
            kvs = list(self.client.get('t1', (u2,)))
        except Exception, exc:
            logger.critical('client failed!', exc_info=True)
            raise exc
        assert len(kvs[0][1]) == 2**20
        o_queue.put((True, u2))
        logger.info('retrievied one_mb at %r', u2)

@pytest.mark.skipif('True')
def test_throughput_join(client):
    '''measure throughput of reading data from t1 by first looking up the
    key in t2, while t2 is under write load
    '''
    client.setup_namespace(dict(t1=1, t2=2))
    
    num_workers = 10
    num_inserts = 100
    total_inserts = num_workers * num_inserts
    data_ids = [uuid.uuid4() for x in xrange(total_inserts)]
    start_time = time.time()
    ret_vals = list(run_many(random_inserts, data_ids, 
                             class_config=client._config,
                             num_workers=num_workers, timeout=total_inserts/2))
    elapsed = time.time() - start_time
    assert len(ret_vals) == total_inserts
    rate = total_inserts / elapsed
    logger.info('%d MB written in %.1f seconds --> %.1f MB/sec across %d workers for storage_type=%s',
                total_inserts, elapsed, rate, num_workers, client._config['storage_type'])

    ## start tool subprocesses that each run a pool
    #writers = multiprocessing.Process(
        #target=run_many,
        #args=(indexer, data_ids, 
              #class_config=client._config,
              #num_workers=num_workers, timeout=total_inserts/2))))

    ### finish writing this test...
