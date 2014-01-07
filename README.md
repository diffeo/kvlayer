kvlayer
=======

Table-oriented abstraction layer over key-value stores, such as
Accumulo, Cassandra, Postgres.  Provides get, put, range scans,
compound UUID keys.

Also contains thrift-based InstanceCollection for storing
self-describing data in a key-value store.  See
[this test of InstanceCollection](src/tests/kvlayer/instance_collection/test_instance_blob_collection.py)
for details.

Accumulo tests
==============

If you run the application tests, a working Accumulo server is required. These
procedure helps you to launch single node and multi node clusters to run the
tests.

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

3. Launch the cluster:

```
make CLUSTER_SIZE=3 cluster
```

CLUSTER_SIZE is the number of instances to launch. Default: 1.

4. Run the tests:

```
py.test -vv src/tests
```

5. Destroy the cluster:

```
make CLUSTER_SIZE=3 cluster-destroy
```

CLUSTER_SIZE *MUST* be the size used in step 3.

To cleanup cluster temporal files in the local machine:

```
make cluster-clean
```

TODO
====

- Modify tests to read the Accumulo cluster address and credentials from an external
  file.
  - Right now the tests point to test-accumulo-1.diffeo.com, update your
    /etc/hosts after you launche the cluster.
- More testing and cleanup. DONE
- Improve documentation. DONE !?
- Cluster destroy. (Looks like there is a bug in salt-cloud when using include).
  - Fixed with a workaround, waiting for the next version of salt which will
    have salt-cloud merged in the Salt project. More information here:
    https://github.com/saltstack/salt/issues/8605

