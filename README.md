kvlayer
=======

Table-oriented abstraction layer over key-value stores, such as
Accumulo, Cassandra, Postgres.  Provides get, put, range scans,
compound UUID keys.

Also contains thrift-based InstanceCollection for storing
self-describing data in a key-value store.  See
[this test of InstanceCollection](src/tests/kvlayer/instance_collection/test_instance_blob_collection.py)
for details.

See details of [testing on Accumulo using saltstack](accumulo-tests.md).

For throughput testing, see [kvlayer_throughput_tests](https://github.com/diffeo/kvlayer/blob/0.4.5/kvlayer/tests/test_throughput.py).

For example, using various single-node EC2 instances, random
reads/writes experiences these rates:

| num_workers | storage_type | read MB/sec | write MB/sec |   |
|-------------|--------------|-------------|--------------|---|
| 100         | redis        | 99.6        | 57.3         |m1.xlarge   |
| 50          | redis        | 93.7        | 56.5         |m1.xlarge   |
| 25          | redis        | 66.9        | 33.8         |m1.xlarge   |
| 80          | postgres     | 34.2        | 14.4         |m1.medium   |
| 50          | postgres     | 33.1        | 14.1         |m1.medium   |
| 25          | postgres     | 30.1        | 13.7         |m1.medium   |
| 100         | accumulo     | 17.2        | 13.6         |m1.large   |
| 50          | accumulo     | 21.9        | 16.0         |m1.large   |
| 25          | accumulo     | 24.7        | 16.6         |m1.large   |

TODO: gather more stats.
