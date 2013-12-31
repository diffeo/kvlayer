kvlayer
=======

Table-oriented abstraction layer over key-value stores, such as
Accumulo, Cassandra, Postgres.  Provides get, put, range scans,
compound UUID keys.

Also contains thrift-based InstanceCollection for storing
self-describing data in a key-value store.  See
[this test of InstanceCollection](kvlayer/src/tests/kvlayer/instance_collection/test_instance_blob_collection.py)
for details.
