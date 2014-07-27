"""test _util key functions

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.

"""

import pytest

from kvlayer._exceptions import BadKey
from kvlayer._utils import serialize_key, deserialize_key
from kvlayer.tests.test_interface import backend, client

def test_serialize_key():
    assert serialize_key(('a', 'b')) == 'a\x00b'

    assert serialize_key(('a', 2), key_spec=(str,int)) == 'a\x0000000000000000000000000000000002'
    assert serialize_key(('a', 2), key_spec=(str,(int,long))) == 'a\x0000000000000000000000000000000002'
    assert serialize_key(('a', 0x12341234123412341234), key_spec=(str,long)) == 'a\x0000000000000012341234123412341234'
    assert serialize_key(('a', 0x12341234123412341234), key_spec=(str,(int,long))) == 'a\x0000000000000012341234123412341234'

    with pytest.raises(BadKey):
        serialize_key(('a', 'b'), key_spec=(str,int))
    with pytest.raises(BadKey):
        serialize_key(('a', 0x12341234123412341234), key_spec=(str,int))

    assert serialize_key(('a foo', 2), key_spec=(str,int)) == 'a foo\x0000000000000000000000000000000002'
    assert serialize_key(('a\x00b',), (str,)) == 'a%00b'
    assert serialize_key(('a%00b',), (str,)) == 'a%2500b'

def test_deserialize_key():
    assert deserialize_key('a%00b', (str,)) == ('a\x00b',)
    assert deserialize_key('a%2500b', (str,)) == ('a%00b',)

def test_serialize_deserialize(client):
    def inversed(frags, specs):
        return deserialize_key(serialize_key(frags, specs), specs)

    for s in ['a\x00b', 'a%00b', 'a%b']:
        assert inversed((s,), (str,)) == (s,)
    for i, s in enumerate(['a\x00b', 'a%00b', 'a%b']):
        assert inversed((s, i), (str, int)) == (s, i)
