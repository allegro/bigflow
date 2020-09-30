import logging
import sys
import os

sys.path.append(os.getcwd())

from bigflow import logger

l = logging.getLogger('logger')
logger.excepthook(l)
if __name__ == '__main__':
    error()