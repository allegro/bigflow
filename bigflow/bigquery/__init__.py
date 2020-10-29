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

# To avoid using pyarrow in the to_dataframe method. Beam requires obsolete version of pyarrow, which is missing a feature
# required by the google-cloud-bigquery
from google.cloud.bigquery import table
table.pyarrow = None