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


import logging
logger = logging.getLogger(__name__)

try:
    # To avoid using pyarrow in the to_dataframe method. Beam requires obsolete version of pyarrow, which is missing a feature
    # required by the google-cloud-bigquery
    # in exception clause because of https://github.com/allegro/bigflow/issues/149
    from google.cloud.bigquery import table
    table.pyarrow = None
except Exception as e:
    logger.warning("Can't import google.cloud.bigquery or google.cloud.bigquery.table.pyarrow property not found.")