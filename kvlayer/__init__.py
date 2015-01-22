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

Backend-specific configuration may also be broken into a separate
section, whose name matches the ``storage_type``:

.. code-block:: yaml

    kvlayer:
      storage_type: redis
      redis:
        storage_addresses: [redis.example.com:6379]
        app_name: app
        namespace: namespace

This is useful if experimenting with different backends, or with the
``split_s3`` hybrid backend.  Any configuration value may be in either
the backend-specific configuration or the top-level kvlayer
configuration, with deeper values taking precedence.

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

postgrest
---------

Uses `PostgreSQL`_ for storage.  This backend is only available if the
:mod:`psycopg2` module is installed.  The ``storage_addresses`` may be
a single `PostgreSQL connection string`_, or may be a ``host:port``
format with additional configuration.  The ``app_name`` and
``namespace`` can only consist of alphanumeric characters,
underscores, or ``$``, and must begin with a letter or underscore.

This backend is newer than, less proven than, and incompatible with the
``postgres`` backend described below.

.. code-block:: yaml

    kvlayer:
      storage_type: postgrest
      storage_addresses: ['postgres.example.com:5432']
      username: test
      password: test
      dbname: test
      # Equivalently, pack this all into a single SQL connection string
      # storage_addresses:
      # - 'host=postgres.example.com port=5432 user=test dbname=test password=test'

      # all of the following parameters are default values and are optional
      # keep this many connections alive
      min_connections: 2
      # never create more than this many connections
      max_connections: 16

The backend assumes the user is able to run SQL ``CREATE TABLE`` and
``DROP TABLE`` statements.  Each kvlayer table is instantiated as an
SQL table named ``appname_namespace_table``.

Within the system, the ``min_connections`` and ``max_connections``
property apply per client object.  If ``min_connections`` is set to 0
then the connection pool will never hold a connection alive, which
typically adds a performance cost to reconnect.

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

riak
----

Uses `Riak`_ for storage.  This backend is only available if the
corresponding :mod:`riak` client library is installed.  Multiple
``storage_addresses`` are actively encouraged for this backend. Each
may be a simple string, or a dictionary containing keys ``host``,
``http_port``, and ``pb_port`` if your setup is using non-standard
port numbers.  A typical setup will look like:

.. code-block:: yaml

    kvlayer:
      storage_type: riak
      storage_addresses: [riak01, riak02, riak03, riak04, riak05]
      # optional settings with their default values
      protocol: pbc # or http or https
      scan_limit: 100

The setup from the Riak "Five-Minute Install" runs five separate
Riak nodes all on localhost, resulting in configuration like

.. code-block: yaml

    kvlayer:
      storage_type: riak
      storage_addresses:
      - { host: "127.0.0.1", pb_port: 10017, http_port: 10018 }
      - { host: "127.0.0.1", pb_port: 10027, http_port: 10028 }
      - { host: "127.0.0.1", pb_port: 10037, http_port: 10038 }
      - { host: "127.0.0.1", pb_port: 10047, http_port: 10048 }
      - { host: "127.0.0.1", pb_port: 10057, http_port: 10058 }

One :mod:`kvlayer` namespace corresponds to one Riak bucket.

The ``protocol`` setting selects between defaulting to the HTTP or
protocol buffer APIs.  While Riak's default is generally ``http``,
the ``pbc`` API seems to work equally well and is much faster.  The
kvlayer backend's default is ``pbc``.

The ``scan_limit`` setting determines how many results will be
returned from each secondary index search.  A higher setting for this
results in fewer network round-trips to get search results, but also
results in higher latency to return each.  This affects both calls to
the kvlayer scan API as well as calls to delete kvlayer tables, which
are also Riak key scans.

Your Riak cluster must be configured with secondary indexing enabled,
and correspondingly, must be using the LevelDB backend.  The default
bucket settings, and in particular setting ``allow_mult`` to
``false``, are correct for :mod:`kvlayer`.

split\_s3
---------

Hybrid backend that stores values for specific tables in `Amazon S3`_,
and pointers to those values and all other tables in some other
:mod:`kvlayer` backend.  This backend is only available if the
:mod:`boto` AWS access library is installed.  Using this effectively
requires knowing the internal details of what table names the end
application will use.  A typical all-Amazon setup would store bulk
objects in S3, and store all other objects in an `Amazon RDS`_
PostgreSQL instance.

.. code-block:: yaml

    kvlayer:
      storage_type: split_s3
      app_name: kvlayer
      namespace: namespace
      split_s3:
        tables: [stream_items]
        aws_access_key_id: Axxxxxxxxxxxxxxxxxxx
        aws_secret_access_key: a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0
        bucket: mybucket
        path_prefix:
        kvlayer_prefix: True
        retries: 5
        retry_interval: 0.1
        kvlayer:
          storage_type: postgres
          storage_addresses:
            - >-
                host=stuff.rds.amazonaws.com port=5432 user=db
                dbname=db password=db

An AWS key must be passed to the backend.  Either an access key ID and
the secret key can be embedded directly in the configuration, as shown
here, or ``aws_access_key_id_path`` and ``aws_secret_access_key_path``
can be names of local files containing those credentials.

Objects are stored in the named ``bucket``, which must exist already.
Each object's key is constructed by taking ``path_prefix`` (if any);
if ``kvlayer_prefix`` is true (default), appending
``app_name/namespace/``; ``table_name/``; and a SHA-256 hash of the
serialized object key, with the first two bytes split off to form a
directory hierarchy.  If your table's schema is ``(str,)`` and you
wrote keys ``a``, ``b``, and ``c`` with the above configuration to
table ``table``, these would be written to ``mybucket`` in respective
S3 object paths

    kvlayer/namespace/table/ca/97/8112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb
    kvlayer/namespace/table/3e/23/e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d
    kvlayer/namespace/table/2e/7d/2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6

The kvlayer keys are written through to the underlying backend, but no
data is written there.  Only tables listed in the ``tables`` parameter
have data stored in S3 (required setting); all other tables are
processed normally.
:meth:`~kvlayer._abstract_storage.AbstractStorage.scan_keys` does not
talk to S3 at all, it just relays the scan from the underlying
backend.  Bulk-delete operations such as
:meth:`~kvlayer._abstract_storage.AbstractStorage.clear_table` and
:meth:`~kvlayer._abstract_storage.AbstractStorage.delete_namespace`
need to do a scan of the underlying table to find the keys to delete.
Particularly if data is being read back immediately after being
written, it is possible that a read (get or scan) operation will fail
to find the just-written object; a failed read for an object expected
to exist will be retried ``retries`` times (default 5), waiting
``retry_interval`` seconds (default 0.1) between each.

The underlying backend is separately configured with its own
``kvlayer`` section inside the backend configuration.

.. _Amazon S3: https://aws.amazon.com/s3/
.. _Amazon RDS: https://aws.amazon.com/rds/

cassandra
---------

Uses the Apache `Cassandra`_ distributed database.  Note that this
backend requires keys to be limited to tuples of UUIDs.

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
are tuples of a fixed length.

.. autofunction:: client

.. autoclass:: kvlayer._abstract_storage.AbstractStorage
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: DatabaseEmpty
.. autoclass:: BadKey

'''

from kvlayer._abstract_storage import COUNTER
from kvlayer._client import client
from kvlayer.config import config_name, default_config, add_arguments, \
    runtime_keys, discover_config, check_config
from kvlayer._exceptions import DatabaseEmpty, BadKey
