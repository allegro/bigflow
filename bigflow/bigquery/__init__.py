import logging
from .interactive import interactive_component as component
from .interactive import add_label, sensor, INLINE_COMPONENT_DATASET_ALIAS
from .dataset_configuration import DatasetConfig
from .job import Job
from .interface import Dataset

__all__ = [
    'component',
    'add_label',
    'sensor',
    'INLINE_COMPONENT_DATASET_ALIAS',
    'DatasetConfig',
    'Job'
]

logger = logging.getLogger(__name__)

try:
    # To avoid using pyarrow in the to_dataframe method. Beam requires obsolete version of pyarrow, which is missing a feature
    # required by the google-cloud-bigquery
    # in exception clause because of https://github.com/allegro/bigflow/issues/149
    from google.cloud.bigquery import table
    table.pyarrow = None
except Exception as e:
    logger.warning("Can't import google.cloud.bigquery or google.cloud.bigquery.table.pyarrow property not found.")