"""test _util key functions

Your use of this software is governed by your license agreement.

Copyright 2012-2015 Diffeo, Inc.

"""

import pytest

from kvlayer.encoders.ascii_percent import AsciiPercentEncoder
from kvlayer._exceptions import BadKey

@pytest.fixture
def encoder():
    return AsciiPercentEncoder()

def test_serialize_key(encoder):
    assert encoder.serialize(('a', 'b'), None) == 'a\x00b'

    assert (encoder.serialize(('a', 2), (str,int)) ==
            'a\x0000000000000000000000000000000002')
    assert (encoder.serialize(('a', 2), (str,(int,long))) ==
            'a\x0000000000000000000000000000000002')
    assert (encoder.serialize(('a', 0x12341234123412341234), (str,long)) ==
            'a\x0000000000000012341234123412341234')
    assert (encoder.serialize(('a', 0x12341234123412341234),
                              (str,(int,long))) ==
            'a\x0000000000000012341234123412341234')

    with pytest.raises(TypeError):
        encoder.serialize(('a', 'b'), (str,int))
    with pytest.raises(TypeError):
        encoder.serialize(('a', 0x12341234123412341234), (str,int))

    assert (encoder.serialize(('a foo', 2), (str,int)) ==
            'a foo\x0000000000000000000000000000000002')
    assert encoder.serialize(('a\x00b',), (str,)) == 'a%00b'
    assert encoder.serialize(('a%00b',), (str,)) == 'a%2500b'

def test_deserialize_key(encoder):
    assert encoder.deserialize('a%00b', (str,)) == ('a\x00b',)
    assert encoder.deserialize('a%2500b', (str,)) == ('a%00b',)

def test_serialize_deserialize(encoder):
    def inversed(frags, specs):
        return encoder.deserialize(encoder.serialize(frags, specs), specs)

    for s in ['a\x00b', 'a%00b', 'a%b']:
        assert inversed((s,), (str,)) == (s,)
    for i, s in enumerate(['a\x00b', 'a%00b', 'a%b']):
        assert inversed((s, i), (str, int)) == (s, i)
