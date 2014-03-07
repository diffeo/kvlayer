"""test _util key functions

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.

"""

import pytest

from kvlayer._exceptions import BadKey
from kvlayer._utils import join_key_fragments

def test_join_key_fragments():
    assert join_key_fragments(('a', 'b')) == 'a\x00b'

    assert join_key_fragments(('a', 2), key_spec=(str,int)) == 'a\x0000000000000000000000000000000002'
    assert join_key_fragments(('a', 2), key_spec=(str,(int,long))) == 'a\x0000000000000000000000000000000002'
    assert join_key_fragments(('a', 0x12341234123412341234), key_spec=(str,long)) == 'a\x0000000000000012341234123412341234'
    assert join_key_fragments(('a', 0x12341234123412341234), key_spec=(str,(int,long))) == 'a\x0000000000000012341234123412341234'

    with pytest.raises(BadKey):
        join_key_fragments(('a', 'b'), key_spec=(str,int))
    with pytest.raises(BadKey):
        join_key_fragments(('a', 0x12341234123412341234), key_spec=(str,int))

    assert join_key_fragments(('a foo', 2), key_spec=(str,int)) == 'a foo\x0000000000000000000000000000000002'
    with pytest.raises(BadKey):
        join_key_fragments(('a\x00foo', 2), key_spec=(str,int))

