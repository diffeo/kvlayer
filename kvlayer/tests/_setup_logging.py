'''
utility for tests that configures logging in roughly the same way that
a program calling kvlayer should setup logging
'''
import logging

logger = logging.getLogger('kvlayer')
logger.setLevel( logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel( logging.DEBUG )
logger.addHandler(ch)
