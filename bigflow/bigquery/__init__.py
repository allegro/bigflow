from bigflow.commons import public

from . import interactive, dataset_configuration, job
from .interactive import INLINE_COMPONENT_DATASET_ALIAS

__all__ = [
    'component',
    'add_label',
    'sensor',
    'INLINE_COMPONENT_DATASET_ALIAS',
    'DatasetConfig',
    'Job'
]


@public(class_alias=True)
class DatasetConfig(dataset_configuration.DatasetConfig): ...


@public(class_alias=True)
class Job(job.Job): ...


@public(alias_for=interactive.add_label)
def add_label(table_name, labels, ds=None): ...


@public(alias_for=interactive.sensor)
def sensor(table_alias, where_clause, ds=None): ...


@public(alias_for=interactive.interactive_component)
def component(**dependencies): ...
