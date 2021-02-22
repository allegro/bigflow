import os
import sys
import json

from .workflow import Workflow, Definition, Job, JobContext
from .configuration import Config

from bigflow._version import __version__


__all__ = [
    # core
    'Workflow',
    'Job',
    'JobContext',
    'Definition',
    'Config',
]


# proactively try to initialize bigflow-specific logging
# it is used to configure logging on pyspark/beam/etc workers
try:
    from bigflow.log import maybe_init_logging_from_env
except ImportError:
    # logging is not installed?
    pass
else:
    maybe_init_logging_from_env()
    del maybe_init_logging_from_env
