import json

from biggerquery.beam_manager import create_dataflow_manager, DataflowManager, TemplatedBeamManager, BeamManager
from biggerquery.test_utils import BeamTestCase
import apache_beam as beam
from apache_beam.options.pipeline_options import \
    PipelineOptions, StandardOptions
from mock import patch

EXAMPLE_SAMPLE_SCHEMA = '''
{"namespace": "example.avro",
 "type": "record",
 "name": "exampleSample",
 "fields": [
     {"name": "id", "type": "string", "mode": "required"},
     {"name": "name", "type": "string", "mode": "required"}
 ]
}
'''

EXAMPLE_OUTPUT_SCHEMA = '''
{"namespace": "example.avro",
 "type": "record",
 "name": "exampleSample",
 "fields": [
     {"name": "id", "type": "string", "mode": "required"},
     {"name": "name", "type": "string", "mode": "required"}
 ]
}
'''


class Data:
    def __init__(self, data):
        self.data = data

    def contains(self, expected_data):
        matching_element = [
            s for s in self.data
            if expected_data['id'] == s['id'] and expected_data['name'] == s['name']
        ]
        return len(matching_element) == 1

    def __len__(self):
        return len(self.data)


class DataFlowManagerTest(BeamTestCase):
    def setUp(self):
        super(DataFlowManagerTest, self).setUp()
        self.data_flow_manager = create_dataflow_manager(
            project_id='project',
            runtime="2019-01-01",
            dataset_name='test_dataset',
            dataflow_bucket="bucket",
            internal_tables=["internal_table"],
            external_tables={"external_table": "not_internal_but_external_table"},
            requirements_file_path="/file_path",
            region="europe",
            machine_type="super fast machine")

    def test_should_create_dataflow_manager(self):
        self.assertIsInstance(self.data_flow_manager, DataflowManager)
        self.assertIsInstance(self.data_flow_manager.beam_manager, TemplatedBeamManager)
        self.assertIsInstance(self.data_flow_manager.beam_manager.beam_manager, BeamManager)

        self.assertEqual(self.data_flow_manager.project_id, 'project')
        self.assertEqual(self.data_flow_manager.dataset, 'test_dataset')
        self.assertEqual(self.data_flow_manager.dataflow_bucket, 'bucket')
        self.assertEqual(self.data_flow_manager.requirements_file_path, '/file_path')
        self.assertEqual(self.data_flow_manager.region, 'europe')
        self.assertEqual(self.data_flow_manager.machine_type, 'super fast machine')

        self.assertEqual(self.data_flow_manager.beam_manager.internal_tables, {'internal_table': 'project.test_dataset.internal_table'})
        self.assertEqual(self.data_flow_manager.beam_manager.external_tables, {"external_table": "not_internal_but_external_table"})
        self.assertEqual(self.data_flow_manager.beam_manager.runtime_str, "2019-01-01")

    def test_should_create_dataflow_runner_pipeline(self):
        # when
        dataflow_pipeline = self.data_flow_manager.create_dataflow_pipeline('fancy-job-name-2019-01-01')
        pipeline_options = dataflow_pipeline.options.display_data()

        # then
        self.assertEqual(pipeline_options['machine_type'], "super fast machine")
        self.assertEqual(pipeline_options['runner'], "DataflowRunner")
        self.assertEqual(pipeline_options['requirements_file'], "/file_path")
        self.assertEqual(pipeline_options['job_name'], "fancy-job-name-2019-01-01")
        self.assertEqual(pipeline_options['temp_location'], "gs://bucket/beam_runner/temp")
        self.assertEqual(pipeline_options['region'], "europe")
        self.assertEqual(pipeline_options['staging_location'], "gs://bucket/beam_runner/staging")
        self.assertEqual(pipeline_options['project'], "project")

    def test_should_create_direct_runner_pipeline(self):
        # when
        dataflow_pipeline = self.data_flow_manager.create_dataflow_pipeline('job', True)

        # then
        pipeline_options = dataflow_pipeline.options.display_data()
        self.assertEqual(len(pipeline_options), 2)
        self.assertEqual(pipeline_options['runner'], "DirectRunner")

    @patch('apache_beam.io.WriteToBigQuery')
    def test_write_truncate_to_big_query(self, write_to_big_query):
        write_to_big_query.return_value = True

        # given
        table_name = 'internal_table'

        # when
        self.data_flow_manager.write_truncate_to_big_query(table_name, json.loads(EXAMPLE_SAMPLE_SCHEMA)['fields'])

        # then
        self.assertEqual(write_to_big_query.call_args[0][0], 'project:test_dataset.internal_table$20190101')
        self.assertEqual(write_to_big_query.call_args[1]['write_disposition'], 'WRITE_TRUNCATE')

        # and
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[0].mode, u'REQUIRED')
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[0].name, u'id')
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[0].type, u'STRING')

        # and
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[1].mode, u'REQUIRED')
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[1].name, u'name')
        self.assertEqual(write_to_big_query.call_args[1]['schema'].fields[1].type, u'STRING')

    def test_project_id(self):
        self.assertEqual(self.data_flow_manager.project_id, 'project')

    def test_run_datetime(self):
        self.assertEqual(self.data_flow_manager.runtime_str, '2019-01-01')

    @patch('apache_beam.io.Read')
    @patch('apache_beam.io.BigQuerySource')
    def test_read_from_big_query(self, big_query_source, read):
        read.return_value = True
        # when
        self.data_flow_manager.read_from_big_query('''
        SELECT * FROM '{internal_table}' (
            first_name STRING,
            last_name STRING
        where batch_date = '{dt}')
        ''')

        # then
        self.assertEqual(big_query_source.call_args[1]['query'], '''
        SELECT * FROM 'project.test_dataset.internal_table' (
            first_name STRING,
            last_name STRING
        where batch_date = \'2019-01-01\')
        ''')
        self.assertEqual(big_query_source.call_args[1]['use_standard_sql'], True)
        self.assertEqual(big_query_source.call_args[1]['project'], 'project')
        self.assertEqual(big_query_source.call_args[1]['dataset'], 'test_dataset')

    @patch('apache_beam.io.ReadFromAvro')
    def test_read_from_avro(self, read_from_avro):
        read_from_avro.return_value = True

        # when
        self.data_flow_manager.read_from_avro("/file_source_path")

        # then
        self.assertEqual(read_from_avro.call_args[0][0], "/file_source_path")

    @patch('apache_beam.io.WriteToAvro')
    def test_write_to_avro(self, write_to_avro):
        write_to_avro.return_value = True

        # when
        self.data_flow_manager.write_to_avro("/file_output_path", EXAMPLE_OUTPUT_SCHEMA)

        # then
        self.assertEqual(write_to_avro.call_args[0][0], "/file_output_path")
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'id'].props['name'], u'id')
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'id'].props[u'mode'], u'required')
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'id'].props['type'].fullname, u'string')

        # and
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'name'].props['name'], u'name')
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'name'].props[u'mode'], u'required')
        self.assertEqual(write_to_avro.call_args[0][1].field_map[u'name'].props['type'].fullname, u'string')

        # and
        self.assertEqual(write_to_avro.call_args[1]['num_shards'], 1)
        self.assertEqual(write_to_avro.call_args[1]['file_name_suffix'], '.avro')
        self.assertEqual(write_to_avro.call_args[1]['shard_name_template'], 'S_N')


class BeamManagerTest(BeamTestCase):
    def setUp(self):
        from apache_beam.testing.test_pipeline import TestPipeline
        super(BeamManagerTest, self).setUp()
        self.dataflow_manager = create_dataflow_manager(
            'test_project',
            '2019-01-01',
            'test_data_set',
            'test_bucket',
            'requirements_file_path',
            'EU',
            'highmem-1',
            None,
            None,
            None)
        options = PipelineOptions()
        options.view_as(StandardOptions).runner = 'DirectRunner'
        self.dataflow_manager.beam_manager.beam_manager.pipeline = TestPipeline(options=options)


class DataFlowManagerIntegrationTest(BeamManagerTest):
    def test_should_process_pipeline_using_avro(self):
        # given
        samples = [{
            'id': 'id-' + str(i),
            'name': 'John Cena'
        } for i in range(5)]

        samples_input_file_path = self.create_avro_test_file(EXAMPLE_SAMPLE_SCHEMA, samples)
        read_from = self.dataflow_manager.read_from_avro(samples_input_file_path)
        output_file_path = self.empty_file('output_file')
        write_to = self.dataflow_manager.write_to_avro(output_file_path, EXAMPLE_OUTPUT_SCHEMA)

        # when
        self.dataflow_manager.beam_manager.beam_manager.pipeline | self.samples(read_from) | write_to
        self.dataflow_manager.beam_manager.beam_manager.pipeline.run().wait_until_finish()
        data = self.extract_data_from_avro(output_file_path)

        # then
        self.assertEqual(len(data), 5)

        # and
        self.assertTrue(data.contains({
            'id': 'id-4',
            'name': 'John Cena'
        }))

    def samples(self, samples):
        class SelectRows(beam.DoFn):
            def process(self, row):
                yield row

        return samples | "select samples" >> beam.ParDo(SelectRows())

    def create_avro_test_file(self, schema, items):
        return self.create_avro_file(schema, items, 'test_file')

    def extract_data_from_avro(self, avro_file_path):
        return Data(self.read_from_avro(avro_file_path))
