import os
print(os.getcwd())

from bigflow import logger

logger.excepthook()
if __name__ == '__main__':
    error()