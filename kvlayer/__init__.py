'''

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

from kvlayer._client import client
from kvlayer.config import config_name, default_config, add_arguments, \
    runtime_keys, check_config
from kvlayer._exceptions import MissingID, DatabaseEmpty, BadKey
