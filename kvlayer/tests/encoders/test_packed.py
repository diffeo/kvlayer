"""test encoders/packed key functions

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.

"""

import os
import random
import struct
import uuid

import pytest

from kvlayer._exceptions import BadKey
from kvlayer.encoders.packed import PackedEncoder
from kvlayer.tests.test_interface import backend, client


def randstr():
    return os.urandom(random.randint(5,50))

def randint():
    return struct.unpack('i', os.urandom(4))[0]

def randlong():
    return struct.unpack('l', os.urandom(8))[0]

def randuuid():
    return uuid.uuid4()

_RAND_TYPE_MAP = {
    str: randstr,
    int: randint,
    long: randlong,
    uuid.UUID: randuuid,
}

def randkey(spec):
    return tuple(_RAND_TYPE_MAP[si]() for si in spec)

_TEST_KEY_SPECS = [
    (int,),
    (str,),
    (str,int),
    (int,str),
    (uuid.UUID,),
    (str,str),
    (str,str,int),
]

_TESTISIZE = 20000

def inner_test_sort_preservation(enc, key_spec, prebuilt_ordered_keys=None):
    if prebuilt_ordered_keys is None:
        a = sorted([randkey(key_spec) for _ in xrange(_TESTISIZE)])
    else:
        a = prebuilt_ordered_keys
    sa = map(lambda x: enc.serialize(x, key_spec), a)
    ssa = sorted(sa)
    assert id(sa) != id(ssa)
    assert sa == ssa, 'sorting serialized form changed order'
    for i in xrange(len(a)):
        pi = enc.deserialize(ssa[i], key_spec)
        # deserialized key is the same value and the same position as
        # original key.
        assert pi == a[i]


@pytest.mark.parametrize("keyspec", [
    (int,),
    (str,),
    (str,int),
    (int,str),
    (int,int),
    (uuid.UUID,),
    (str,str),
    (str,str,int),
])
def test_packed_specs(keyspec):
    enc = PackedEncoder()
    inner_test_sort_preservation(enc, keyspec)

def test_packed_si1():
    enc = PackedEncoder()
    inner_test_sort_preservation(
        enc,
        (str,int),
        [
            ('\0',1),
            ('\0\0',2),
            ('\0\x01',3),
            ('\x01',4),
            ('\x01\0', 5),
            ('\x01#aoeu', 6),
            ('\x02',7),
        ]
    )
