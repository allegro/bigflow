import avro.schema

import avro.schema
import apache_beam as beam
from apache_beam.options.pipeline_options import \
    PipelineOptions, StandardOptions
import avro
from pathlib import Path
from biggerquery.test_utils import BeamTestCase
from biggerquery.user_commons.fastai import predict_job


def example_input_and_expected_output():
    example_input = [
        {
            'pk': '1',
            'age': 50,
            'workclass': 'Private',
            'fnlwgt': 236746,
            'education': 'Masters',
            'education-num': 14.0,
            'marital-status': 'Divorced',
            'occupation': 'Exec-managerial',
            'relationship': 'Not-in-family',
            'race': 'White',
            'sex': 'Male',
            'capital-gain': 10520,
            'capital-loss': 0,
            'hours-per-week': 45,
            'native-country': 'United-States'
        },
        {
            'pk': '2',
            'age': 38,
            'workclass': 'Private',
            'fnlwgt': 96185,
            'education': 'HS-grad',
            'education-num': 1.0,
            'marital-status': 'Divorced',
            'occupation': 'Exec-managerial',
            'relationship': 'Unmarried',
            'race': 'Black',
            'sex': 'Female',
            'capital-gain': 0,
            'capital-loss': 0,
            'hours-per-week': 40,
            'native-country': 'United-States'
        }
    ]
    expected_output = [
        {
            'pk': '1',
            'age': 50,
            'workclass': 'Private',
            'fnlwgt': 236746,
            'education': 'Masters',
            'education-num': 14.0,
            'marital-status': 'Divorced',
            'occupation': 'Exec-managerial',
            'relationship': 'Not-in-family',
            'race': 'White',
            'sex': 'Male',
            'capital-gain': 10520,
            'capital-loss': 0,
            'hours-per-week': 45,
            'native-country': 'United-States',
            'prediction': 1
        },
        {
            'pk': '2',
            'age': 38,
            'workclass': 'Private',
            'fnlwgt': 96185,
            'education': 'HS-grad',
            'education-num': 1.0,
            'marital-status': 'Divorced',
            'occupation': 'Exec-managerial',
            'relationship': 'Unmarried',
            'race': 'Black',
            'sex': 'Female',
            'capital-gain': 0,
            'capital-loss': 0,
            'hours-per-week': 40,
            'native-country': 'United-States',
            'prediction': 0
        },
    ]
    return example_input, expected_output


def avro_input(file_path):
    return beam.io.ReadFromAvro(file_path)


def avro_output(file_path, schema):
    return beam.io.WriteToAvro(
        file_path,
        avro.schema.Parse(schema),
        num_shards=1,
        use_fastavro=False,
        shard_name_template='S_N',
        file_name_suffix='.avro')


def local_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'
    return beam.Pipeline(options=options)


INPUT_SCHEMA = '''
{"namespace": "example.avro",
 "type": "record",
 "name": "MetricSample",
 "fields": [
     {"name": "pk", "type": "string"},
     {"name": "age", "type": "float"},
     {"name": "workclass", "type": "string"},
     {"name": "fnlwgt", "type": "float"},
     {"name": "education", "type": "string"},
     {"name": "education-num", "type": "float"},
     {"name": "marital-status", "type": "string"},
     {"name": "occupation", "type": "string"},
     {"name": "relationship", "type": "string"},
     {"name": "race", "type": "string"},
     {"name": "sex", "type": "string"},
     {"name": "capital-gain", "type": "float"},
     {"name": "capital-loss", "type": "float"},
     {"name": "hours-per-week", "type": "float"},
     {"name": "native-country", "type": "string"}
 ]
}
'''

OUTPUT_SCHEMA = '''
{"namespace": "example.avro",
 "type": "record",
 "name": "MetricSample",
 "fields": [
     {"name": "pk", "type": "string"},
     {"name": "age", "type": "float"},
     {"name": "workclass", "type": "string"},
     {"name": "fnlwgt", "type": "float"},
     {"name": "education", "type": "string"},
     {"name": "education-num", "type": "float"},
     {"name": "marital-status", "type": "string"},
     {"name": "occupation", "type": "string"},
     {"name": "relationship", "type": "string"},
     {"name": "race", "type": "string"},
     {"name": "sex", "type": "string"},
     {"name": "capital-gain", "type": "float"},
     {"name": "capital-loss", "type": "float"},
     {"name": "hours-per-week", "type": "float"},
     {"name": "native-country", "type": "string"},
     {"name": "prediction", "type": "int"}
 ]
}
'''


class TestPredictE2E(BeamTestCase):

    def test_should_make_prediction(self):
        # given
        example_input, expected_output = example_input_and_expected_output()
        example_input_avro = self.create_avro_file(INPUT_SCHEMA, example_input, 'test_should_make_prediction')
        output_avro = self.empty_file('test_should_make_prediction')

        job = predict_job.FastaiTabularPredictionJob(
            input_table_name=None,
            output_table_name=None,
            dataset=None,
            partition_column=None,
            custom_input_collection=avro_input(example_input_avro),
            custom_output=avro_output(output_avro, OUTPUT_SCHEMA),
            custom_pipeline=local_pipeline(),
            model_file_path=str((Path(__file__).parent / 'model.pkl').absolute()))

        # when
        job.run('2019-01-01')

        # then
        self.assertEqual(self.read_from_avro(output_avro), expected_output)

    def test_should_accept_empty_collection(self):
        # given
        example_input, expected_output = [], []
        example_input_avro = self.create_avro_file(INPUT_SCHEMA, example_input, 'test_should_accept_empty_collection')
        output_avro = self.empty_file('test_should_accept_empty_collection')

        job = predict_job.FastaiTabularPredictionJob(
            input_table_name=None,
            output_table_name=None,
            dataset=None,
            partition_column=None,
            custom_input_collection=avro_input(example_input_avro),
            custom_output=avro_output(output_avro, OUTPUT_SCHEMA),
            custom_pipeline=local_pipeline(),
            model_file_path=str((Path(__file__).parent / 'model.pkl').absolute()))

        # when
        job.run('2019-01-01')

        # then
        self.assertEqual(self.read_from_avro(output_avro), expected_output)