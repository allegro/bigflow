from . import resources
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
    'resources'
]

try:
    from . import bigquery
    __all__.append('bigquery')
except ImportError:
    pass

try:
    from . import monitoring
    __all__.append('monitoring')
except ImportError:
    pass
