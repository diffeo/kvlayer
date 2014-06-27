'''Database abstraction for key/value stores.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Many popular large-scale databases export a simple key/value
abstraction: the database is simply a list of cells with some
(possibly structured) key and a value for each key.  This allows the
database system itself to partition the database in some way, and in a
distributed system it allows the correct system hosting a specific key
to be found easily.  This model is also simple enough that it can be
used with in-memory storage or more traditional SQL-based databases.

This module provides a simple abstraction around these
key/value-oriented databases.  It works with :mod:`yakonfig` to hold
basic configuration settings, so the top-level YAML configuration must
have a ``kvlayer`` block.  This will typically look something like

.. code-block:: yaml

    kvlayer:
      storage_type: redis
      storage_addresses: [redis.example.com:6379]
      app_name: app
      namespace: namespace

These four parameters are always required.  ``storage_type`` gives one
of the database backends described below.  ``storage_addresses`` is a
list of backend-specific database locations.  ``app_name`` and
``namespace`` combine to form a container for the virtual tables
stored in kvlayer.

Backends
========

local
-----

This is intended only for testing.  Values are stored in a Python
dictionary and not persisted.  ``storage_addresses`` are not required.

.. code-block:: yaml

    kvlayer:
      storage_type: local

filestorage
-----------

This is intended only for testing.  Values are stored in a local file
using the :mod:`shelve` module.  This does not require
``storage_addresses``, but it does require:

.. code-block:: yaml

    kvlayer:
      storage_type: filestorage

      # Name of the file to use for storage
      filename: /tmp/kvlayer.bin

      # If set, actually work on a copy of "filename" at this location.
      copy_to_filename: /tmp/kvlayer-copy.bin

redis
-----

Uses the `Redis`_ in-memory database.

.. code-block:: yaml

    kvlayer:
      storage_type: redis

      # host:port locations of Redis servers; only the first is used
      storage_addresses: [redis.example.com:6379]

      # Redis database number (default: 0)
      redis_db_num: 1

accumulo
--------

Uses the Apache `Accumulo`_ distributed database.  Your installation
must be running the Accumulo proxy, and the configuration points at
that proxy.

.. code-block:: yaml

    kvlayer:
      storage_type: accumulo

      # host:port location of the proxy; only the first is used
      storage_addresses: [accumulo.example.com:50096]
      username: root
      password: secret

      # all of the following parameters are default values and are optional
      accumulo_max_memory: 1000000
      accumulo_timeout_ms: 30000
      accumulo_threads: 10
      accumulo_latency_ms: 10
      thrift_framed_transport_size_in_mb: 15

Each kvlayer table is instantiated as an Accumulo table named
``appname_namespace_table``.

.. _Accumulo: http://accumulo.apache.org/

postgres
--------

Uses `PostgreSQL`_ for storage.  This backend is only available if the
:mod:`psycopg2` module is installed.  The ``storage_type`` is a
`PostgreSQL connection string`_.  The ``app_name`` and ``namespace``
can only consist of alphanumeric characters, underscores, or ``$``,
and must begin with a letter or underscore.

.. code-block:: yaml

    kvlayer:
      storage_type: postgres
      storage_addresses:
      - 'host=postgres.example.com port=5432 user=test dbname=test password=test'

      # all of the following parameters are default values and are optional
      # keep this many connections alive
      min_connections: 2
      # never create more than this many connections
      max_connections: 16
      # break large scans (using SQL) into chunks of this many
      scan_inner_limit: 1000

The backend assumes the user is able to run SQL ``CREATE TABLE`` and
``DROP TABLE`` statements.  Each kvlayer namespace is instantiated as
an SQL table named ``kv_appname_namespace``; kvlayer tables are
collections of rows within the namespace table sharing a common field.

Within the system, the ``min_connections`` and ``max_connections``
property apply per client object.  If ``min_connections`` is set to 0
then the connection pool will never hold a connection alive, which
typically adds a performance cost to reconnect.

.. _PostgreSQL: http://www.postgresql.org
.. _PostgreSQL connection string: http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS

cassandra
---------

Uses the Apache `Cassandra`_ distributed database.

.. code-block:: yaml

    kvlayer:
      storage_type: cassandra
      storage_addresses: ['cassandra.example.com:9160']
      username: root
      password: secret

      connection_pool_size: 2
      max_consistency_delay: 120
      replication_factor: 1
      thrift_framed_transport_size_in_mb: 15

.. _Cassandra: http://cassandra.apache.org/

API
===

Having set up the global configuration, it is enough to call
:func:`kvlayer.client` to get a storage client object.

The API works in terms of "tables", though these are slightly
different from tradational database tables.  Each table has keys which
are tuples of a fixed length.  Tuple members must be
:class:`uuid.UUID` objects unless the system is configured with
``keys_must_be_uuid: false``.

.. autofunction:: client

.. autoclass:: kvlayer._abstract_storage.AbstractStorage
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: DatabaseEmpty
.. autoclass:: BadKey

Instance Collections
====================

.. automodule:: kvlayer.instance_collection

'''

from kvlayer._client import client
from kvlayer.config import config_name, default_config, add_arguments, \
    runtime_keys, check_config
from kvlayer._exceptions import DatabaseEmpty, BadKey
