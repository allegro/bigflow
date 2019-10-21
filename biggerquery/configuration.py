from __future__ import absolute_import

from .gcp_defaults import DEFAULT_REGION
from .gcp_defaults import DEFAULT_LOCATION
from .gcp_defaults import DEFAULT_MACHINE_TYPE
from .utils import unzip_file_and_save_outside_zip_as_tmp_file


class DatasetConfig(object):

    def __init__(self,
                 project_id,
                 dataset_name,
                 internal_tables=None,
                 external_tables=None,
                 credentials=None,
                 extras=None,
                 dataflow_config=None,
                 location=DEFAULT_LOCATION):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.internal_tables = internal_tables or []
        self.external_tables = external_tables or {}
        self.credentials = credentials or None
        self.extras = extras or {}
        self.dataflow_config = dataflow_config
        self.location = location

    def _as_dict(self, with_dataflow_config=False):
        config = {
            'project_id': self.project_id,
            'dataset_name': self.dataset_name,
            'internal_tables': self.internal_tables,
            'external_tables': self.external_tables,
            'credentials': self.credentials,
            'extras': self.extras,
            'location': self.location
        }
        if self.dataflow_config and with_dataflow_config:
            config.update(self.dataflow_config._as_dict())
        return config


class DataflowConfig(object):

    def __init__(self,
                 dataflow_bucket_id,
                 requirements_path,
                 region=DEFAULT_REGION,
                 machine_type=DEFAULT_MACHINE_TYPE):
        self.dataflow_bucket_id = dataflow_bucket_id
        self.requirements_path = requirements_path
        self.region = region
        self.machine_type = machine_type
        self._tmp_requirements_file = None

    @property
    def _final_requirements_path(self):
        if '.zip' in self.requirements_path:
            self._tmp_requirements_file = unzip_file_and_save_outside_zip_as_tmp_file(self.requirements_path)
            return self._tmp_requirements_file.name
        return self.requirements_path

    def _as_dict(self):
        return {
            'dataflow_bucket': self.dataflow_bucket_id,
            'machine_type': self.machine_type,
            'region': self.region,
            'requirements_file_path': self._final_requirements_path
        }