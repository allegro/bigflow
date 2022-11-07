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


@public(class_alias=True)
class Definition(bigflow.workflow.Definition): ...


@public(class_alias=True)
class Job(bigflow.workflow.Job): ...


@public(class_alias=True)
class JobContext(bigflow.workflow.JobContext): ...


@public(class_alias=True)
class Workflow(bigflow.workflow.Workflow): ...


@public(class_alias=True)
class Config(bigflow.configuration.Config): ...
