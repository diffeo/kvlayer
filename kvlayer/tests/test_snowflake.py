'''Tests for :mod:`kvlayer.snowflake`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import random
import time

import pytest

from kvlayer._local_memory import LocalStorage
from kvlayer.snowflake import Snowflake


@pytest.fixture
def client():
    c = LocalStorage()
    c._data = {}
    c.setup_namespace({'t': (long,)})
    return c


def test_snowflake_explicit():
    s = Snowflake(identifier=1, sequence=0)
    assert s(now=0x12345678) == 0xedcba98800010000
    assert s(now=0x12345678) == 0xedcba98800010001
    assert s(now=0x12345679) == 0xedcba98700010002


def test_snowflake_implicit(monkeypatch):
    # the most random number
    monkeypatch.setattr(random, 'randint', lambda lo, hi: 17)
    monkeypatch.setattr(time, 'time', lambda: 0x12345678)

    s = Snowflake()
    assert s() == 0xedcba98800110011
    assert s() == 0xedcba98800110012
    assert s() == 0xedcba98800110013


def test_snowflake_wraparound():
    s = Snowflake(identifier=0, sequence=0xFFFF)
    assert s(now=0x12345678) == 0xedcba9880000ffff
    assert s(now=0x12345678) == 0xedcba98800000000


def test_snowflake_scan(client):
    s = Snowflake(identifier=0, sequence=0)
    client.put('t', ((s(now=0x12345678),), 'older'))
    client.put('t', ((s(now=0x12345679),), 'newer'))
    assert [v for k, v in client.scan('t')] == ['newer', 'older']


def test_snowflake_scan_wraparound(client):
    s = Snowflake(identifier=0xFFFF, sequence=0xFFFF)
    client.put('t', ((s(now=0x12345678),), 'older'))
    client.put('t', ((s(now=0x12345679),), 'newer'))
    assert [v for k, v in client.scan('t')] == ['newer', 'older']


def test_snowflake_scan_sequence(client):
    # You can't make an ordering guarantee in general.  We could try
    # to order things so that things usually appear in reverse order
    # of calls to the generator, but then someone will depend on it
    # and one time in 2**16 this will go wrong.  This is the
    # _de facto_ behavior:
    s = Snowflake(identifier=0, sequence=0)
    client.put('t', ((s(now=0x12345678),), 'oldest'))
    client.put('t', ((s(now=0x12345678),), 'older'))
    client.put('t', ((s(now=0x12345679),), 'newer'))
    client.put('t', ((s(now=0x12345679),), 'newest'))
    assert ([v for k, v in client.scan('t')] ==
            ['newer', 'newest', 'oldest', 'older'])
