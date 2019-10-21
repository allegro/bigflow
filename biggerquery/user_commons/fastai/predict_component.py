import runpy
from pathlib import Path
from ...utils import unzip_file_and_save_outside_zip_as_tmp_file


class FastaiTabularPredictionComponent(object):
    def __init__(self,
                 input_table_name,
                 output_table_name,
                 model_file_path,
                 custom_input_collection=None,
                 custom_output=None,
                 custom_pipeline=None):
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name
        self.model_file_path = model_file_path
        self.custom_input_collection = custom_input_collection
        self.custom_output = custom_output
        self.custom_pipeline = custom_pipeline

    def __call__(self, dm):
        predict_path = str((Path(__file__).parent/'predict.py').absolute())
        with open(unzip_file_and_save_outside_zip_as_tmp_file(self.model_file_path).name, 'rb') as model:
            model_bytes = model.read()

        return runpy.run_path(
            path_name=unzip_file_and_save_outside_zip_as_tmp_file(predict_path, suffix='.py').name,
            init_globals={'run_kwargs': {
                'p': self.custom_pipeline,
                'input_collection': self.custom_input_collection,
                'output': self.custom_output,
                'model_bytes': model_bytes,
            }},
            run_name='__main__')