kvlayer
=======

``kvlayer`` is a database abstraction layer providing a simple key-value store to applications.  For development purposes this can run against an in-memory implementation or a server such as [Redis](http://redis.io/); in test and production, this can be switched to a relational database such as [PostgreSQL](http://postgresql.org/) or a cluster database such as [Accumulo](http://accumulo.apache.org/) or [Riak](http://basho.com/riak/).

Configuration
-------------

``kvlayer`` depends on the [Yakonfig](https://github.com/diffeo/yakonfig/) library to get its configuration information.  Configuration is in a YAML file passed into the application.  This includes a *storage type*, indicating which backend to use, and an *application name* and a *namespace*, both of which distinguish different applications sharing the same database.

```yaml
    kvlayer:
      storage_type: local  # in-memory data store
      app_name: kvlayer
      namespace: kvlayer
```

kvlayer API
-----------

Applications see multiple kvlayer *tables*, which may be implemented as database-native tables for databases that have that concept.  Each row has a key and a value.  The keys are Python tuples, with some consistent set of types; tuple parts may be strings, integers, or UUIDs.  Values are always Python byte strings.

There are four basic operations kvlayer makes available.  ``put()`` writes one or more key-value pairs into the database.  ``get()`` retrieves key-value pairs with known fixed keys.  ``scan()`` retrieves key-value pairs within a range of keys.  ``delete()`` removes specific keys.

A minimal kvlayer application would look like:

```python
    import argparse
    import kvlayer
    import yakonfig

    parser = argparse.ArgumentParser()
    yakonfig.parse_args(parser, [yakonfig, kvlayer])
    kvl = kvlayer.client()
    kvl.setup_namespace({'table': (str,)})

    # Write values
    kvl.put('table', (('foo',), 'one'), (('bar',), 'two'))

    # Retrieve values
    for k,v in kvl.get('table', ('foo',)):
      assert k == 'foo'
      print v

    # Scan values
    for k,v in kvl.scan('table', (('a',), ('e',))):
      print k
      print v

    # Scan keys
    for k in kvl.scan_keys('table', (('e',), ('z',))):
      print k

    # Delete values
    kvl.delete('table', ('foo',))
```

Other notes
-----------

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
