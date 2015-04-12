'''Tuple-to-string encoders.

.. This software is released under an MIT/X11 open source license.
   Copyright 2015 Diffeo, Inc.

This module provides converters between :mod:`kvlayer` key tuples and
flat byte strings.  These are needed by some backends that only store
single string keys.

.. automodule:: kvlayer.encoders.ascii_percent

'''
from __future__ import absolute_import

from kvlayer.encoders.ascii_percent import AsciiPercentEncoder
from kvlayer.encoders.packed import PackedEncoder

def get_encoder(name=None):
    '''Get an encoder instance by name.

    If `name` is :const:`None`, use a default encoder.

    :param str name: name of the encoder
    :return: instance of the encoder
    :raise KeyError: if `name` is not a valid encoder
    
    '''
    if name is None:
        name = 'ascii_percent'
    encoders = dict([(encoder.config_name, encoder)
                     for encoder in [AsciiPercentEncoder, PackedEncoder]])
    return encoders[name]()
