import logging
from .bigflow.bigflow.logger import excepthook

logger = logging.getLogger('logger')
excepthook(logger)


if __name__ == '__main__':
    error()