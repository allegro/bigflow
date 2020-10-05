import logging
import sys
import os

sys.path.append(os.getcwd())

from bigflow import log

l = logging.getLogger('logger')
log.excepthook(l)
if __name__ == '__main__':
    error()