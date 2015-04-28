'''Implementation of AbstractStorage using a proxy server running a CBOR RPC protocol

CBOR RPC server for talking to Java databases like Accumulo:
https://github.com/diffeo/kvlayer-java-proxy

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''

from __future__ import absolute_import
import json
import logging
import random
import socket
import struct
import time
import threading

import cbor

from kvlayer._abstract_storage import StringKeyedStorage, COUNTER
from kvlayer._exceptions import ProgrammerError

logger = logging.getLogger(__name__)


class SocketReader(object):
    '''
    Simple adapter from socket.recv to file-like-read
    '''
    def __init__(self, sock):
        self.socket = sock
        self.timeout_seconds = 10.0

    def read(self, num):
        start = time.time()
        data = self.socket.recv(num)
        while len(data) < num:
            now = time.time()
            if now > (start + self.timeout_seconds):
                break
            ndat = self.socket.recv(num - len(data))
            if ndat:
                data += ndat
        return data


class CborRpcClient(object):
    '''Base class for all client objects.

    This provides common `addr_family`, `address`, and `registry_addresses`
    configuration parameters, and manages the connection back to the server.

    Automatic retry and time based fallback is managed from
    configuration parameters `retries` (default 5), and
    `base_retry_seconds` (default 0.5). Retry time doubles on each
    retry. E.g. try 0; wait 0.5s; try 1; wait 1s; try 2; wait 2s; try
    3; wait 4s; try 4; wait 8s; try 5; FAIL. Total time waited just
    under base_retry_seconds * (2 ** retries).

    .. automethod:: __init__
    .. automethod:: _rpc
    .. automethod:: close

    '''

    def __init__(self, config=None):
        self._socket_family = config.get('addr_family', socket.AF_INET)
        # may need to be ('host', port)
        self._socket_addr = config.get('address')
        if self._socket_family == socket.AF_INET:
            if not isinstance(self._socket_addr, tuple):
                # python socket standard library insists this be tuple!
                tsocket_addr = tuple(self._socket_addr)
                assert len(tsocket_addr) == 2, ('address must be length-2 '
                                                'tuple ("hostname", '
                                                'port number), got {0!r} '
                                                'tuplified to {1!r}'
                                                .format(self._socket_addr,
                                                        tsocket_addr))
                self._socket_addr = tsocket_addr
        self._socket = None
        self._rfile = None
        self._local_addr = None
        self._message_count = 0
        self._retries = config.get('retries', 5)
        self._base_retry_seconds = float(config.get('base_retry_seconds', 0.5))

    def _conn(self):
        # lazy socket opener
        if self._socket is None:
            try:
                self._socket = socket.create_connection(self._socket_addr)
                self._local_addr = self._socket.getsockname()
            except:
                logger.error('error connecting to %r:%r', self._socket_addr[0],
                             self._socket_addr[1], exc_info=True)
                raise
        return self._socket

    def close(self):
        '''Close the connection to the server.

        The next RPC call will reopen the connection.

        '''
        if self._socket is not None:
            self._rfile = None
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except socket.error:
                logger.warn('error closing lockd client socket',
                            exc_info=True)
            self._socket = None

    @property
    def rfile(self):
        if self._rfile is None:
            conn = self._conn()
            self._rfile = SocketReader(conn)
        return self._rfile

    def _rpc(self, method_name, params):
        '''Call a method on the server.

        Calls ``method_name(*params)`` remotely, and returns the results
        of that function call.  Expected return types are primitives, lists,
        and dictionaries.

        :raise Exception: if the server response was a failure

        '''
        ## it's really time and file-space consuming to log all the
        ## rpc data, but there are times when a developer needs to.
        mlog = None
        #mlog = logging.getLogger('cborrpc')
        tryn = 0
        delay = self._base_retry_seconds
        self._message_count += 1
        message = {
            'id': self._message_count,
            'method': method_name,
            'params': params
        }
        if mlog is not None:
            mlog.debug('request %r', message)
        buf = cbor.dumps(message)

        errormessage = None
        while True:
            try:
                conn = self._conn()
                conn.send(buf)
                response = cbor.load(self.rfile)
                if mlog is not None:
                    mlog.debug('response %r', response)
                assert response['id'] == message['id']
                if 'result' in response:
                    return response['result']
                # From here on out we got a response, the server
                # didn't have some weird intermittent error or
                # non-connectivity, it gave us an error message. We
                # don't retry that, we raise it to the user.
                errormessage = response.get('error')
                if errormessage and hasattr(errormessage, 'get'):
                    errormessage = errormessage.get('message')
                if not errormessage:
                    errormessage = repr(response)
                break
            except Exception as ex:
                if tryn < self._retries:
                    tryn += 1
                    logger.debug('ex in %r (%s), retrying %s in %s sec...',
                                 method_name, ex, tryn, delay, exc_info=True)
                    self.close()
                    time.sleep(delay)
                    delay *= 2
                    continue
                logger.error('failed in rpc %r %r', method_name, params,
                             exc_info=True)
                raise
        raise Exception(errormessage)


class CborProxyPooledConnection(object):
    """CborProxyPooledConnection is a context manager
    usage:
        with pooled_conn as conn:
           conn._rpc(...)
    """

    def __init__(self, zk_addresses, proxy_addresses, username, password, instance_name):
        self._zk_addresses = zk_addresses
        self._proxy_addresses = proxy_addresses
        self._username = username
        self._password = password
        self._instance_name = instance_name
        self._conn = None  # CborRpcClient instance
        self.lock = threading.Lock()
        self.owned = False
        self.lastused = None # time.time() when last used. close if unused 30s

    def conn(self):
        if not self._conn:
            host, port = random.choice(self._proxy_addresses)
            logger.debug('connecting to cbor proxy %s:%s', host, port)
            self._conn = CborRpcClient({'address': (host, port), 'retries': 0})
            zk_addr = random.choice(self._zk_addresses)
            ok, msg = self._conn._rpc(
                u'connect',
                [unicode(zk_addr),
                 unicode(self._username),
                 unicode(self._password),
                 unicode(self._instance_name)])
            if not ok:
                raise Exception(msg)
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.conn()

    def __exit__(self, exc_type, exc_value, traceback):
        # todo, detect broken connection in exc_*
        with self.lock:
            self.owned = False
            self.lastused = time.time()
            if (exc_value is not None) and (self._conn is not None):
                self._conn.close()
                self._conn = None


class CborProxyConnectionPool(object):
    def __init__(self):
        # dict from (big key of params) to [list of connections]
        self.they = {}
        self.lastgc = None
        self.lock = threading.Lock()

    def connect(self, zk_addresses, proxy_addresses, username, password, instance_name):
        """Returns a CborProxyPooledConnection. Use in a with stamement.
        with pooled_connection as conn:
          conn.foo()
        """
        with self.lock:
            # drop stale connections _before_ we try to use them.
            self.maybegc()
            out = self._connect(zk_addresses, proxy_addresses, username, password, instance_name)
            return out

    def _connect(self, zk_addresses, proxy_addresses, username, password, instance_name):
        key = json.dumps((zk_addresses, proxy_addresses, username, password, instance_name))
        cons = self.they.get(key)
        if cons:
            for con in cons:
                if not con.owned:
                    with con.lock:
                        if not con.owned:
                            con.owned = True
                            return con
        newpc = CborProxyPooledConnection(zk_addresses, proxy_addresses, username, password, instance_name)
        newpc.owned = True
        if cons is None:
            cons = [newpc]
            self.they[key] = cons
        else:
            cons.append(newpc)
        return newpc

    def maybegc(self):
        # garbage collect old connections, at most every 5s
        now = time.time()
        if (self.lastgc is None) or (self.lastgc < (now - 5)):
            self.gc()
            self.lastgc = now

    def gc(self):
        # garbage collect old connections
        now = time.time()
        too_old = now - 30
        for k, cons in self.they.iteritems():
            # keep if owned, or not unused yet, or used recently enough
            pos = 0
            while pos < len(cons):
                c = cons[pos]
                if c.owned or (c.lastused is None) or (c.lastused > too_old):
                    # keep, move on
                    pos += 1
                else:
                    cons.pop(pos)


class CborProxyStorage(StringKeyedStorage):
    '''

    Required config terms:
    zk_addresses: [list, of, addresses, to, zookeeper, servers]
    proxy_addresses: [list, of, 'host:port', for, cborproxy, servers]
    instance_name: 'accumulo' or 'instance' are common accumulo cluster names
    username: database credential
    password: database credential
    '''
    global_pool = CborProxyConnectionPool()

    def __init__(self, *args, **kwargs):
        super(CborProxyStorage, self).__init__(*args, **kwargs)

        zk_addresses = self._config.get('zk_addresses', None)
        if not zk_addresses:
            raise ProgrammerError('config lacks zk_addresses')
        proxy_addresses = self._config.get('proxy_addresses', None)
        if not proxy_addresses:
            raise ProgrammerError('config lacks proxy_addresses')

        def str_to_pair(address):
            if ':' not in address:
                return (address, 7123)
            else:
                h, p = address.split(':')
                return (h, int(p))
        self._zk_addresses = zk_addresses  # map(str_to_pair, zk_addresses)
        self._proxy_addresses = map(str_to_pair, proxy_addresses)

        # cached lazy connection
        #self._conn = None
        # have we sent zk connection information to proxy?
        self._proxy_connected = False

        self._connection_pool = kwargs.get('pool') or CborProxyStorage.global_pool

    def pooled_conn(self):
        # return a CborProxyConnectionPool, use in with
        return self._connection_pool.connect(self._zk_addresses, self._proxy_addresses, self._config.get('username'), self._config.get('password'), self._config.get('instance_name'))

    def _ns(self, table):
        return '%s_%s_%s' % (self._app_name, self._namespace, table)

    def _translate_value_config(self, value_types):
        # convert setup_namespace value_types for upload to java proxy
        if value_types is None:
            return None
        out = {}
        for k,v in value_types.iteritems():
            out[unicode(self._ns(k))] = self._translate_value_type(v)
        return out

    def _translate_value_type(self, value_type):
        # convert one of setup_namespace value_types for upload to java proxy
        if value_type is None:
            return None
        if isinstance(value_type, type):
            return unicode(value_type.__name__)
        else:
            return unicode(type(value_type).__name__)

    def setup_namespace(self, table_names, value_types=None):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        super(CborProxyStorage, self).setup_namespace(table_names, value_types)
        simple_table_names = dict([(unicode(self._ns(k)), dict())
                                   for k in table_names.iterkeys()])
        with self.pooled_conn() as conn:
            conn._rpc(u'setup_namespace', [simple_table_names, self._translate_value_config(value_types)])

    def delete_namespace(self):
        simple_table_names = dict([(self._ns(k), dict())
                                   for k in self._table_names.iterkeys()])
        with self.pooled_conn() as conn:
            conn._rpc(u'delete_namespace', [simple_table_names])

    def clear_table(self, table_name):
        table_name = self._ns(table_name)
        with self.pooled_conn() as conn:
            conn._rpc(u'clear_table', [unicode(table_name)])

    def _put(self, table_name, keys_and_values):
        with self.pooled_conn() as conn:
            conn._rpc(u'put',
                       [unicode(self._ns(table_name)), keys_and_values])

    def _scan(self, table_name, key_ranges):
        table_name = self._ns(table_name)
        if not key_ranges:
            key_ranges = [(None, None)]
        cmd = [unicode(table_name), key_ranges]
        with self.pooled_conn() as conn:
            while cmd is not None:
                resultob =  conn._rpc(u'scan', cmd)
                results = None
                next_command = None
                if isinstance(resultob, list):
                    results = resultob
                if isinstance(resultob, dict):
                    results = resultob.get('out')
                    next_command = resultob.get('next')
                if results:
                    for k,v in results:
                        yield k,v
                cmd = next_command

    def _scan_keys(self, table_name, key_ranges):
        table_name = self._ns(table_name)
        if not key_ranges:
            key_ranges = [(None, None)]
        cmd = [unicode(table_name), key_ranges]
        with self.pooled_conn() as conn:
            while cmd is not None:
                resultob = conn._rpc(u'scan_keys', cmd)
                results = None
                next_command = None
                if isinstance(resultob, list):
                    results = resultob
                elif isinstance(resultob, dict):
                    results = resultob.get('out')
                    next_command = resultob.get('next')
                else:
                    raise Exception('bad result from scan_keys, got {}'.format(type(resultob)))
                if results:
                    for k in results:
                        yield k
                cmd = next_command

    def _get(self, table_name, keys):
        table_name = self._ns(table_name)
        with self.pooled_conn() as conn:
            return conn._rpc(u'get', [unicode(table_name), keys])

    def close(self):
        # everything is taken care of at pooled_conn context scope
        pass

    def _delete(self, table_name, keys, **kwargs):
        table_name = self._ns(table_name)
        with self.pooled_conn() as conn:
            conn._rpc(u'delete', [unicode(table_name), keys])

    def increment(self, table_name, *keys_and_values):
        assert self._value_types[table_name] is COUNTER, 'attempting to increment non-COUNTER table {}'.format(table_name)

        # do the transformation like put()
        convkv = []
        for (k, v) in keys_and_values:
            self.check_put_key_value(k, v, table_name)
            nk = self._encoder.serialize(k, self._table_names[table_name])
            nv = self.value_to_str(v, self._value_types[table_name])
            convkv.append( (nk , nv) )

        table_name = self._ns(table_name)

        with self.pooled_conn() as conn:
            conn._rpc(u'increment', [unicode(table_name), convkv])

    def shutdown_proxies(self):
        for host, port in self._proxy_addresses:
            xc = CborRpcClient({'address': (host, port), 'retries': 0})
            try:
                xc._rpc(u"shutdown", [])
            except EOFError:
                pass  # ok
            except:
                logger.info("%s:%s shutdown", host, port, exc_info=True)

    def value_to_str(self, value, value_type):
        # override StringKeyedStorage part for translating values in put()
        if value is None:
            return None
        # LongCombiner expects a big-endian 8-byte int
        if value_type is COUNTER:
            return struct.pack('>q', value)
        return super(CborProxyStorage, self).value_to_str(value, value_type)
