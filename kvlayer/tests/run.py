#!/usr/bin/env python
"""kvlayer self-tests.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import argparse
import os
import subprocess
import sys

try:
    import pytest
except ImportError:
    pytest = None

import kvlayer.tests.performance as performance_tests


def main():
    parser = argparse.ArgumentParser(description='Run all kvlayer tests.')
    if pytest:
        parser.add_argument('--unit', action='store_true',
                            help='Run unit tests')
    parser.add_argument('--performance', '--perf', action='store_true',
                        help='Run performance tests')
    parser.add_argument('redis_address', metavar='HOST:PORT',
                        help='location of a redis instance to use for testing')
    args = parser.parse_args()

    if not args.unit and not args.performance:
        args.unit = (pytest is not None)
        args.performance = True

    rc = 0

    if args.unit:
        # Since the performance tests do complicated multiprocessing
        # things, and things like the pytest-cov extension register
        # multiprocessing hooks to cooperate with pytest-xdist, make
        # sure py.test runs in an isolated process environment.
        ret = subprocess.call([sys.executable, '-m', 'py.test',
                               '--runslow', '--runperf',
                               '--redis-address', args.redis_address,
                               '-k', 'not (cassandra or accumulo or postgres or cborproxy)',
                               os.path.dirname(__file__)])
        if rc == 0:
            rc = ret

    if args.performance:
        ret = performance_tests.run_all_perftests(args.redis_address)
        if rc == 0:
            rc = ret

    return rc

if __name__ == '__main__':
    sys.exit(main())
