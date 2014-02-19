#!/usr/bin/env python
"""kvlayer self-tests.

-----

Your use of this software is governed by your license agreement.

Copyright 2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import argparse
import os
import sys

import pytest

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('redis_address', metavar='HOST:PORT',
                        help='location of a redis instance to use for testing')
    args = parser.parse_args()
    test_dir = os.path.dirname(__file__)
    response = pytest.main(['-v', '-v',
                            '--redis-address', args.redis_address,
                            '-k', 'not (cassandra or accumulo or postgres)',
                            test_dir])
    sys.exit(response)

if __name__ == '__main__':
    main()
