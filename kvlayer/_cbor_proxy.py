'''Implementation of AbstractStorage using a proxy server running a CBOR RPC protocol

CBOR RPC server for talking to Java databases like Accumulo:
https://github.com/diffeo/kvlayer-java-proxy

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''

from __future__ import absolute_import
import logging
import random
import socket
import time

import cbor

from kvlayer._abstract_storage import StringKeyedStorage
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
        mlog = logging.getLogger('cborrpc')
        tryn = 0
        delay = self._base_retry_seconds
        self._message_count += 1
        message = {
            'id': self._message_count,
            'method': method_name,
            'params': params
        }
        mlog.debug('request %r', message)
        buf = cbor.dumps(message)

        errormessage = None
        while True:
            try:
                conn = self._conn()
                conn.send(buf)
                response = cbor.load(self.rfile)
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


# def _rangesk(key, key_spec):
#     if not key:
#         return None
#     return serialize_key(key, key_spec=key_spec)


class CborProxyStorage(StringKeyedStorage):
    '''

    Required config terms:
    zk_addresses: [list, of, addresses, to, zookeeper, servers]
    proxy_addresses: [list, of, 'host:port', for, cborproxy, servers]
    instance_name: 'accumulo' or 'instance' are common accumulo cluster names
    username: database credential
    password: database credential
    '''
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
        self._conn = None
        # have we sent zk connection information to proxy?
        self._proxy_connected = False

    @property
    def conn(self):
        if not self._conn:
            host, port = random.choice(self._proxy_addresses)
            logger.debug('connecting to cbor proxy %s:%s', host, port)
            self._conn = CborRpcClient({'address': (host, port)})
            zk_addr = random.choice(self._zk_addresses)
            ok, msg = self._conn._rpc(
                u'connect',
                [unicode(zk_addr),
                 unicode(self._config.get('username')),
                 unicode(self._config.get('password')),
                 unicode(self._config.get('instance_name'))])
            if not ok:
                raise Exception(msg)
        return self._conn

    def _ns(self, table):
        return '%s_%s_%s' % (self._app_name, self._namespace, table)

    def setup_namespace(self, table_names, value_types=None):
        '''creates tables in the namespace.  Can be run multiple times with
        different table_names in order to expand the set of tables in
        the namespace.
        '''
        super(CborProxyStorage, self).setup_namespace(table_names, value_types)
        simple_table_names = dict([(self._ns(k), dict())
                                   for k in table_names.iterkeys()])
        self.conn._rpc(u'setup_namespace', [simple_table_names])

    def delete_namespace(self):
        simple_table_names = dict([(self._ns(k), dict())
                                   for k in self._table_names.iterkeys()])
        self.conn._rpc(u'delete_namespace', [simple_table_names])

    def clear_table(self, table_name):
        table_name = self._ns(table_name)
        self.conn._rpc(u'clear_table', [unicode(table_name)])

    def _put(self, table_name, keys_and_values):
        self.conn._rpc(u'put',
                       [unicode(self._ns(table_name)), keys_and_values])

    def _scan(self, table_name, key_ranges):
        table_name = self._ns(table_name)
        if not key_ranges:
            key_ranges = [(None, None)]
        return self.conn._rpc(u'scan', [unicode(table_name), key_ranges])

    def _scan_keys(self, table_name, key_ranges):
        table_name = self._ns(table_name)
        if not key_ranges:
            key_ranges = [(None, None)]
        return self.conn._rpc(u'scan_keys', [unicode(table_name), key_ranges])

    def _get(self, table_name, keys):
        table_name = self._ns(table_name)
        return self.conn._rpc(u'get', [unicode(table_name), keys])

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _delete(self, table_name, keys, **kwargs):
        table_name = self._ns(table_name)
        self.conn._rpc(u'delete', [unicode(table_name), keys])

    def shutdown_proxies(self):
        for host, port in self._proxy_addresses:
            xc = CborRpcClient({'address': (host, port), 'retries': 0})
            try:
                xc._rpc(u"shutdown", [])
            except EOFError:
                pass  # ok
            except:
                logger.info("%s:%s shutdown", host, port, exc_info=True)
