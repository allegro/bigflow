from bigflow._version import __version__
from bigflow.commons import public

import bigflow.workflow
import bigflow.configuration


__all__ = [
    'Workflow',
    'Job',
    'JobContext',
    'Definition',
    'Config',
]


@public(alias_for=bigflow.workflow.Definition)
class Definition(bigflow.workflow.Definition): ...


@public(alias_for=bigflow.workflow.Job)
class Job(bigflow.workflow.Job): ...


@public(alias_for=bigflow.workflow.JobContext)
class JobContext(bigflow.workflow.JobContext): ...


@public(alias_for=bigflow.workflow.Workflow)
class Workflow(bigflow.workflow.Workflow): ...


@public(alias_for=bigflow.configuration.Config)
class Config(bigflow.configuration.Config): ...


# proactively try to initialize bigflow-specific logging
# it is used to configure logging on pyspark/beam/etc workers
try:
    from bigflow.log import maybe_init_logging_from_env
except ImportError:
    pass  # logging is not installed?
else:
    maybe_init_logging_from_env()
    del maybe_init_logging_from_env
