import logging
import os
print(os.getcwd())

from bigflow import logger

l = logging.getLogger('logger')
logger.excepthook(l)
if __name__ == '__main__':
    error()