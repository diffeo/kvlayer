'''Snowflake key generator.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.

.. autoclass:: Snowflake

'''
from __future__ import absolute_import, division, print_function
import math
import random
import time


class Snowflake(object):
    '''Snowflake key generator.

    This is a callable object that returns :class:`long` values
    that can be used as :mod:`kvlayer` keys.  These keys have the
    property that the most-recently-written keys appear first in a
    table scan, and that different hosts will probably write
    differently-valued keys.

    The keys are made up from the current timestamp, an
    object-specific identifier, and a sequence number.  For best
    results a single key generator should be shared across all parts
    of a single task or program.  For instance, if this is used in
    connection with :mod:`rejester`, the low-order bits of the current
    worker ID could be used to generate the initial identifier and
    sequence number.

    .. see:: https://blog.twitter.com/2010/announcing-snowflake

    '''
    # pylint: disable=too-few-public-methods

    def __init__(self, identifier=None, sequence=None):
        if identifier is None:
            identifier = random.randint(0, 65535)
        if sequence is None:
            sequence = random.randint(0, 65535)

        #: Identifier part of the key.  This is a 16-bit unsigned
        # integer.
        self.identifier = identifier

        #: Current sequence number.  This is a 16-bit unsigned integer.
        # Calling the snowflake key generator will return a key
        # with the current sequence number, then increment the
        # sequence number, wrapping around.
        self.sequence = sequence

    def __call__(self, now=None):
        if now is None:
            now = time.time()
        # Lower-numbered keys sort first, so negate the time so that
        # newest is first.
        #
        # This is the bit distribution from the Twitter blog post.
        # I'm worried that it's not "right"; consider that 256
        # randomly-chosen 16-bit numbers have about a 40% chance
        # of having some collision.  Randomly choosing both the
        # identifier and sequence number makes this much less likely
        # (0.00076%).  Does adding milliseconds (or 2**-10-seconds)
        # but taking that many bits out of the sequence number
        # improve this?
        key = ((((-math.trunc(now)) & 0xFFFFFFFF) << 32) |
               ((self.identifier & 0xFFFF) << 16) |
               (self.sequence & 0xFFFF))
        self.sequence = (self.sequence + 1) & 0xFFFF
        return key
