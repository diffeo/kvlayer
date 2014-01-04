kvlayer
=======

Table-oriented abstraction layer over key-value stores, such as
Accumulo, Cassandra, Postgres.  Provides get, put, range scans,
compound UUID keys.

Also contains thrift-based InstanceCollection for storing
self-describing data in a key-value store.  See
[this test of InstanceCollection](src/tests/kvlayer/instance_collection/test_instance_blob_collection.py)
for details.

Tests
=====

Only Accumulo on EC2 is supported right now. All this procedure was tested on
Ubuntu 12.04.

1. Create the file ~/.saltcloud-ec2.conf with your EC2 credentials:

```
my-amz-credentials:
  provider: ec2
  id: YOUR_EC2_ID
  key: YOUR_EC2_KEY
  private_key: /home/you/.accumulo-saltstack.pem
  keyname: accumulo-saltstack
```

NOTE: Don't change the name "my-amz-credentials"!

2. Make sure that your ssh private_key is in the specified path with permissions
   600.

3. Launch the cluster

```
make CLUSTER_SIZE=3 cluster
```

CLUSTER_SIZE is the number of instances to launch. Default: 1.

TODO:

- Cluster destroy. (Looks like there is a bug in salt-cloud when using include).
- Improve documentation.
- Modify tests to read the Accumulo cluster address and credentials from an external
  file.
- More testing and cleanup.

