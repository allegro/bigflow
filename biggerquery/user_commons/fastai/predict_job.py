import runpy
import uuid
from pathlib import Path
from ...utils import unzip_file_and_save_outside_zip_as_tmp_file
from . import predict_io


class FastaiTabularPredictionJob(object):
    def __init__(self,
                 input_table_name,
                 output_table_name,
                 partition_column,
                 model_file_path,
                 dataset,
                 torch_package_path,
                 fastai_package_path,
                 dataflow_job_name=None,
                 custom_input_collection=None,
                 custom_output=None,
                 custom_pipeline=None):
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name
        self.partition_column = partition_column
        self.model_file_path = model_file_path
        self.config = dataset.config if dataset is not None else None
        self.torch_package_path = torch_package_path
        self.fastai_package_path = fastai_package_path
        self.dataflow_job_name = dataflow_job_name or f'prediction-on-{input_table_name}-{uuid.uuid4()}'

        self.custom_input_collection = custom_input_collection
        self.custom_output = custom_output
        self.custom_pipeline = custom_pipeline

    def run(self, dt):
        predict_path = str((Path(__file__).parent / 'predict.py').absolute())
        with open(unzip_file_and_save_outside_zip_as_tmp_file(self.model_file_path).name, 'rb') as model:
            model_bytes = model.read()

        return runpy.run_path(
            path_name=unzip_file_and_save_outside_zip_as_tmp_file(predict_path, suffix='.py').name,
            init_globals={'run_kwargs': {
                'p': self.custom_pipeline or predict_io.dataflow_pipeline(
                    self.config,
                    self.dataflow_job_name,
                    self.torch_package_path,
                    self.fastai_package_path),
                'input_collection': self.custom_input_collection or predict_io.bigquery_input(
                    self.config.project_id,
                    self.config.dataset_name,
                    self.input_table_name,
                    dt,
                    self.partition_column),
                'output': self.custom_output or predict_io.bigquery_output(
                    self.config.project_id,
                    self.config.dataset_name,
                    self.output_table_name,
                    dt),
                'model_bytes': model_bytes,
            }},
            run_name='__main__')