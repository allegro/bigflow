import os
import sys
import json
import warnings
import importlib

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


def __getattr__(name):
    if name in {
        'bigquery',
        'monitoring',
        'resources',
    }:
        importlib.import_module(f"bigflow.{name}")
        msg = f"Don't use `bigflow.{name}` directly, add explicit import `import bigflow.{name}` instead"
        warnings.warn(msg, DeprecationWarning)
        print("!!!", msg, file=sys.stderr)
        return globals()[name]
    raise AttributeError


def _maybe_init_logging_from_env():

    try:
        from bigflow import log
    except ImportError:
        print("bigflow[log] is not installed", file=sys.stderr)
        return

    if 'bf_log_config' not in os.environ:
        return

    log_config = os.environ.get('bf_log_config', "{}")
    try:
        log_config = json.loads(log_config)
    except ValueError as e:
        print("invalid 'log_config' json:", e, file=sys.stderr)        
        return

    if 'workflow_id' in log_config:
        workflow_id = log_config['workflow_id']
    else:
        workflow_id = os.environ.get('bf_workflow_id')

    log.init_logging(log_config, workflow_id or 'none', banner=False)


# proactively try to initialize bigflow-specific logging
# it is used to configure logging on pyspark/beam/etc workers
_maybe_init_logging_from_env()
